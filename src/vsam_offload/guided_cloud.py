"""Azure SDK boundary. No connection strings, provisioning, or local fallback."""
from __future__ import annotations

import json
import os
import secrets
import threading
import time
from contextlib import contextmanager
from urllib.parse import quote, urlsplit

from azure.core import MatchConditions
from azure.core.exceptions import AzureError, HttpResponseError, ResourceExistsError, ResourceNotFoundError

from .guided_pipeline import GuidedError, encoded
from .sync_store import CosmosStore

CLEANUP_TIMEOUT = 10


class ConfigurationError(GuidedError):
    def __init__(self):
        super().__init__(503, "Configuração Azure incompleta ou inválida; contate o administrador.")


class ManifestLease:
    def __init__(self, blob, lease):
        self.blob, self.lease, self.etag = blob, lease, None
        self.stopped = threading.Event()
        self.failure = None

    def renew(self):
        while not self.stopped.wait(15):
            try:
                self.lease.renew(timeout=10)
            except Exception as error:
                # Background failures must close the write gate, not disappear in a thread.
                self.failure = error
                return

    def check(self):
        if self.failure:
            raise GuidedError(409, "A concessão de exclusividade expirou; tente novamente.") from self.failure

    def load(self):
        self.check()
        response = self.blob.download_blob(lease=self.lease, timeout=10)
        self.etag = response.properties.etag
        return json.loads(response.readall())

    def save(self, run):
        self.check()
        try:
            result = self.blob.upload_blob(encoded(run), overwrite=True, lease=self.lease, etag=self.etag,
                                           match_condition=MatchConditions.IfNotModified, timeout=10)
        except HttpResponseError as error:
            if error.status_code in (409, 412):
                self.failure = error
                raise GuidedError(409, "A execução mudou ou perdeu a concessão; tente novamente.") from error
            raise
        self.etag = result["etag"]


class AzureCloud:
    def __init__(self):
        required = ("STORAGE_ACCOUNT_URL", "COSMOS_ENDPOINT", "EVENTHUB_FULLY_QUALIFIED_NAMESPACE",
                    "AZURE_SUBSCRIPTION_ID", "AZURE_RESOURCE_GROUP", "AZURE_LOCATION", "AZURE_WEBAPP_NAME")
        if any(not os.getenv(name) for name in required):
            raise ConfigurationError()
        self.env = {name: os.environ[name] for name in required}
        self.env["AZURE_COMPUTE_KIND"] = os.getenv("AZURE_COMPUTE_KIND", "appservice")
        if self.env["AZURE_COMPUTE_KIND"] not in {"containerapp", "appservice"}:
            raise ConfigurationError()
        for name in ("STORAGE_ACCOUNT_URL", "COSMOS_ENDPOINT"):
            parsed = urlsplit(self.env[name])
            if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
                    or parsed.query or parsed.fragment or parsed.path not in ("", "/")):
                raise ConfigurationError()
        namespace = self.env["EVENTHUB_FULLY_QUALIFIED_NAMESPACE"]
        if any(char in namespace for char in "/\\:@?# \r\n"):
            raise ConfigurationError()
        # Import only on explicit production initialization, never substitute test storage.
        from azure.cosmos import CosmosClient
        from azure.eventhub import EventHubConsumerClient, EventHubProducerClient
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import BlobServiceClient

        self.credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
        self.blobs = BlobServiceClient(self.env["STORAGE_ACCOUNT_URL"], credential=self.credential,
                                       connection_timeout=10, read_timeout=15, retry_total=2)
        self.raw = self.blobs.get_container_client(os.getenv("RAW_CONTAINER", "vsam-raw"))
        self.control = self.blobs.get_container_client(os.getenv("CONTROL_CONTAINER", "demo-control"))
        self.cosmos = CosmosClient(self.env["COSMOS_ENDPOINT"], credential=self.credential,
                                   connection_timeout=10, read_timeout=15, retry_total=2)
        self.database_name = os.getenv("COSMOS_DATABASE", "mainframeOffload")
        self.container_name = os.getenv("COSMOS_CONTAINER", "balances")
        database = self.cosmos.get_database_client(self.database_name)
        self.store = CosmosStore(database.get_container_client(self.container_name),
                                database.get_container_client(os.getenv("COSMOS_QUARANTINE_CONTAINER", "sync-quarantine")))
        self.hub = os.getenv("EVENTHUB_NAME", "vsam-changes")
        self.group = os.getenv("EVENTHUB_CONSUMER_GROUP", "guided-demo")
        if self.group != "guided-demo":
            raise ConfigurationError()
        self.stream = f"{namespace}/{self.hub}"
        options = dict(fully_qualified_namespace=namespace, eventhub_name=self.hub,
                       credential=self.credential, auth_timeout=10, socket_timeout=0.5, retry_total=1)
        # Short socket polls let the receive loop observe shutdown promptly.
        # auth_timeout and the supervised operation deadline are separate bounds.
        self._producer = lambda: EventHubProducerClient(**options)
        self._consumer = lambda: EventHubConsumerClient(consumer_group=self.group, **options)
        self._closing_lock, self._closing = threading.Lock(), []

    def allocate_accounts(self, run_id, count):
        accounts = []
        for _ in range(count):
            for attempt in range(10):
                account = f"{secrets.randbelow(10 ** 12):012d}"
                try:
                    # A conditional reservation prevents cross-run collision in the 12-digit keyspace.
                    self.control.get_blob_client(f"account-keys/{account}.json").upload_blob(
                        encoded({"runId": run_id}), overwrite=False, timeout=10)
                    accounts.append(account)
                    break
                except ResourceExistsError:
                    continue
            else:
                raise GuidedError(503, "Não foi possível reservar novas contas sintéticas.")
        return accounts

    def _manifest(self, run_id):
        return self.control.get_blob_client(f"runs/{run_id}.json")

    def create_manifest(self, run):
        self._manifest(run["runId"]).upload_blob(encoded(run), overwrite=False, timeout=10)

    def load_manifest(self, run_id):
        try:
            return json.loads(self._manifest(run_id).download_blob(timeout=10).readall())
        except ResourceNotFoundError as error:
            raise GuidedError(404, "Execução desconhecida.") from error

    def list_manifests(self):
        items = sorted(self.control.list_blobs(name_starts_with="runs/", timeout=10),
                       key=lambda blob: blob.last_modified, reverse=True)[:20]
        return [json.loads(self.control.get_blob_client(item.name).download_blob(timeout=10).readall())
                for item in items]

    @contextmanager
    def lock(self, run_id):
        from azure.storage.blob import BlobLeaseClient
        blob = self._manifest(run_id)
        lease = BlobLeaseClient(blob)
        try:
            lease.acquire(lease_duration=60, timeout=10)
        except ResourceNotFoundError as error:
            raise GuidedError(404, "Execução desconhecida.") from error
        except HttpResponseError as error:
            if error.status_code in (409, 412):
                raise GuidedError(409, "Esta execução já está em andamento.") from error
            raise
        control = ManifestLease(blob, lease)
        renewer = threading.Thread(target=control.renew, name="guided-lease-renew", daemon=True)
        renewer.start()
        try:
            yield control
        finally:
            control.stopped.set()
            renewer.join(timeout=20)
            try:
                lease.release(timeout=10)
            except AzureError:
                # Finite leases recover after host death; failed renewal already fences saves.
                pass

    def get(self, name):
        try:
            return self.raw.get_blob_client(name).download_blob(timeout=10).readall()
        except ResourceNotFoundError as error:
            raise GuidedError(404, "Artefato ainda não disponível.") from error

    def info(self, name):
        blob = self.raw.get_blob_client(name)
        properties = blob.get_blob_properties(timeout=10)
        return dict(url=blob.url, name=name, etag=properties.etag, byteCount=properties.size,
                    container=self.raw.container_name)

    def put(self, name, raw, media="application/octet-stream"):
        from azure.storage.blob import ContentSettings
        try:
            self.raw.get_blob_client(name).upload_blob(
                raw, overwrite=False, content_settings=ContentSettings(content_type=media), timeout=10)
        except ResourceExistsError:
            if self.get(name) != raw:
                raise GuidedError(409, "Um artefato imutável possui conteúdo diferente.")
        return self.info(name)

    def starting_positions(self):
        with self._producer() as producer:
            return {partition: producer.get_partition_properties(partition)["last_enqueued_sequence_number"]
                    for partition in producer.get_partition_ids()}

    def send(self, event):
        from azure.eventhub import EventData
        with self._producer() as producer:
            message = EventData(encoded(event))
            message.properties = {"runId": event["runId"], "phase": event["phase"]}
            batch = producer.create_batch(partition_key=event["accountId"])
            batch.add(message)
            producer.send_batch(batch, timeout=20)

    def consume(self, positions, on_message, done, timeout=45):
        with self._closing_lock:
            self._closing = [worker for worker in self._closing if worker.is_alive()]
            if self._closing:
                raise GuidedError(503, "Um consumidor anterior ainda está encerrando; aguarde para retomar.")
        if done():
            return
        stopped, errors, workers, clients = threading.Event(), [], [], []
        gate = threading.RLock()

        def fail(error):
            with gate:
                if not stopped.is_set():
                    errors.append(error)
                    stopped.set()

        def callback(context, event):
            with gate:
                if stopped.is_set() or event is None:
                    return
                try:
                    if event.offset is None or event.sequence_number is None:
                        raise GuidedError(502, "Mensagem sem posição de transporte.")
                    on_message(context.partition_id, event.offset, event.sequence_number, b"".join(event.body))
                    if done():
                        stopped.set()
                except Exception as error:
                    fail(error)

        def receiver(client, partition, start, ready):
            def on_event(context, event):
                ready.set()
                callback(context, event)

            def on_error(context, error):
                ready.set()
                fail(error)

            try:
                client.receive(on_event=on_event, on_error=on_error,
                               on_partition_initialize=lambda context: ready.set(),
                               partition_id=partition, starting_position=start,
                               starting_position_inclusive=False, max_wait_time=1, prefetch=50)
            except Exception as error:
                fail(error)
            finally:
                ready.set()

        def close_receiver(client, ready, worker):
            # SDK close() before processor startup can miss stop(). Retain a supervisor
            # even after the HTTP deadline; callbacks are already fenced by stopped.
            ready.wait()
            try:
                client.close()
            except Exception as error:
                errors.append(error)
            worker.join()

        try:
            for partition, start in positions.items():
                client, ready = self._consumer(), threading.Event()
                clients.append((client, ready))
                worker = threading.Thread(target=receiver, args=(client, partition, start, ready),
                                          name=f"guided-receive-{partition}", daemon=True)
                workers.append(worker)
                worker.start()
            stopped.wait(timeout)
        finally:
            with gate:
                stopped.set()
            closers = [threading.Thread(target=close_receiver, args=(client, ready, worker),
                                        name="guided-receive-supervisor", daemon=True)
                       for (client, ready), worker in zip(clients, workers)]
            with self._closing_lock:
                self._closing.extend(closers)
                for closer in closers:
                    closer.start()
            deadline = time.monotonic() + CLEANUP_TIMEOUT
            for closer in closers:
                closer.join(timeout=max(0, deadline - time.monotonic()))
        if errors:
            raise GuidedError(502, "Falha no consumo do Event Hubs; retome esta etapa.") from errors[0]
        if any(closer.is_alive() for closer in closers):
            raise GuidedError(503, "O consumidor não encerrou dentro do prazo.")
        if not done():
            raise GuidedError(502, "Prazo de consumo esgotado; tente novamente antes de expirar a retenção.")

    def config(self):
        env = self.env
        root = f"/subscriptions/{env['AZURE_SUBSCRIPTION_ID']}/resourceGroups/{env['AZURE_RESOURCE_GROUP']}/providers/"
        portal = lambda resource: "https://portal.azure.com/#resource" + quote(root + resource, safe="/")
        app = env["AZURE_WEBAPP_NAME"]
        storage = urlsplit(env["STORAGE_ACCOUNT_URL"]).hostname.split(".")[0]
        cosmos = urlsplit(env["COSMOS_ENDPOINT"]).hostname.split(".")[0]
        namespace = env["EVENTHUB_FULLY_QUALIFIED_NAMESPACE"].split(".")[0]
        container_app = env.get("AZURE_COMPUTE_KIND") == "containerapp"
        provider = "Microsoft.App/containerApps" if container_app else "Microsoft.Web/sites"
        compute_type = "Azure Container Apps" if container_app else "Azure App Service Linux"
        host = dict(name=app, type=compute_type, portalUrl=portal(f"{provider}/{app}"))
        services = [
            dict(id="origin", name="SIMULADOR cp037", type=f"Simulador em {compute_type}", portalUrl=host["portalUrl"],
                 role="Gera dados sintéticos; nenhum mainframe conectado",
                 details=dict(host=app, format="cp037 / COMP-3", recordBytes=51)),
            dict(id="compute", **host, role="Executa cada etapa somente após clique",
                 details=dict(region=env["AZURE_LOCATION"])),
            dict(id="storage", name=storage, type="Azure Blob Storage",
                 portalUrl=portal(f"Microsoft.Storage/storageAccounts/{storage}"), role="Bytes e evidências imutáveis",
                 details=dict(account=storage, container=self.raw.container_name)),
            dict(id="eventhubs", name=self.hub, type="Azure Event Hubs",
                 portalUrl=portal(f"Microsoft.EventHub/namespaces/{namespace}/eventhubs/{self.hub}"), role="Transporte real de eventos",
                 details=dict(namespace=namespace, eventHub=self.hub, consumerGroup=self.group, partitionKey="accountId")),
            dict(id="cosmos", name=self.container_name, type="Azure Cosmos DB",
                 portalUrl=portal(f"Microsoft.DocumentDB/databaseAccounts/{cosmos}"), role="Destino durável com CAS",
                 details=dict(account=cosmos, database=self.database_name, container=self.container_name,
                              partitionKey="/accountId")),
            dict(id="control", name=self.control.container_name, type="Azure Blob Storage",
                 portalUrl=portal(f"Microsoft.Storage/storageAccounts/{storage}"), role="Manifestos, leases e checkpoints por execução",
                 details=dict(account=storage, container=self.control.container_name)),
        ]
        return dict(mode="azure", host=host, services=services, location=env["AZURE_LOCATION"])

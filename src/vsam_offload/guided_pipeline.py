"""Click-driven synthetic capture; durable evidence, never a native VSAM adapter."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
import logging
import threading
import time
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

from .copybook import parse_copybook, parse_record, record_length
from .generate_sample import SAMPLE_ROWS, build_record
from .parse_vsam import normalize_document
from .sync_contract import DEFAULT_COPYBOOK, EPOCH, canonical, make_event
from .sync_engine import SyncEngine

logger = logging.getLogger(__name__)
STEP_DEFINITIONS = [
    ("source", "Gerar origem simulada", "SIMULADOR no serviço de computação Azure cria bytes cp037; não acessa VSAM.", ["origin", "storage"]),
    ("transfer", "Transferir bytes", "Cópia real entre blobs, sem interpretação dos registros.", ["compute", "storage"]),
    ("parse", "Interpretar copybook", "Decodificação dos bytes recebidos, incluindo COMP-3 exato.", ["compute", "storage"]),
    ("publish", "Publicar eventos", "Envelopes reais no Event Hubs, particionados por conta.", ["compute", "eventhubs", "control"]),
    ("apply", "Aplicar no Cosmos DB", "Consumo real do Event Hubs e gravação condicional durável.", ["compute", "eventhubs", "cosmos", "control"]),
    ("verify", "Conferir resultado", "Leituras pontuais do Cosmos comparadas à origem independente.", ["compute", "cosmos"]),
]
STEPS = [item[0] for item in STEP_DEFINITIONS]
LIMITATIONS = [
    "Origem SIMULADA no serviço de computação Azure; não há captura nativa, conexão ou CDC de VSAM.",
    "Orquestração manual: pausas entre cliques fazem parte do tempo observado; não é SLA.",
    "Entrega pelo menos uma vez; versões e CAS do SyncEngine protegem o destino contra duplicatas.",
    "Checkpoints duráveis são exclusivos desta execução/fase; não representam o avanço de outros consumidores.",
    "A retenção do Event Hubs limita retomadas; blobs e checkpoints não recriam mensagens expiradas.",
]


class GuidedError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status, self.message = status, message


def now():
    return datetime.now(timezone.utc).isoformat()


def validate_run_id(value):
    try:
        if isinstance(value, str) and str(UUID(value)) == value:
            return value
    except ValueError:
        pass
    raise GuidedError(400, "Identificador de execução inválido.")


def encoded(value):
    return canonical(value).encode("utf-8")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def public(run):
    return copy.deepcopy({key: value for key, value in run.items() if not key.startswith("_")})


def path(run, kind):
    prefix, filename = {
        "source": ("source", "records.bin"), "landing": ("landing", "records.bin"),
        "parsed": ("parsed", "records.json"), "events": ("parsed", "events.json"),
        "metadata": ("source", "metadata.json"),
    }[kind]
    return f"{prefix}/{run['runId']}/phase-{run['phase']}/{filename}"


class GuidedPipeline:
    """The cloud dependency is real in production; explicit injection is for tests."""

    def __init__(self, cloud):
        self.cloud = cloud
        self.fields = parse_copybook(DEFAULT_COPYBOOK)
        self.size = record_length(self.fields)
        self.engine = SyncEngine(cloud.store, stream=cloud.stream, group=cloud.group)

    def config(self):
        return dict(self.cloud.config(), steps=[
            dict(id=key, title=title, description=description, serviceIds=services)
            for key, title, description, services in STEP_DEFINITIONS
        ], limitations=LIMITATIONS)

    def list_runs(self):
        return [public(run) for run in self.cloud.list_manifests()]

    def get_run(self, run_id):
        return public(self.cloud.load_manifest(validate_run_id(run_id)))

    def create_run(self):
        run_id, stamp = str(uuid4()), now()
        accounts = self.cloud.allocate_accounts(run_id, len(SAMPLE_ROWS))
        rows = [dict(row, account_id=account, sequence_number=1)
                for row, account in zip(SAMPLE_ROWS, accounts)]
        run = dict(runId=run_id, phase=1, stage=0, createdAt=stamp, updatedAt=stamp,
                   phaseCreatedAt=stamp, steps={}, artifacts={}, history=[],
                   _expected={row["account_id"]: row for row in rows},
                   _primaryAccount=rows[0]["account_id"])
        self._prepare(run, rows, stamp)
        self.cloud.create_manifest(run)
        return public(run)

    def _prepare(self, run, rows, stamp):
        run.update(_sourceRows=copy.deepcopy(rows), _metadata=[
            dict(accountId=row["account_id"], eventId=str(uuid4()),
                 sourceVersion=row["sequence_number"], previousVersion=row["sequence_number"] - 1,
                 committedAt=stamp) for row in rows
        ], _received={}, _checkpoints={})
        run.pop("_publish", None)

    def _artifact_link(self, run, kind):
        run["artifacts"][kind] = f"/api/runs/{run['runId']}/artifacts/{kind}"
        return run["artifacts"][kind]

    def step(self, run_id, step):
        validate_run_id(run_id)
        if step not in STEPS:
            raise GuidedError(404, "Etapa desconhecida.")
        index = STEPS.index(step)
        with self.cloud.lock(run_id) as control:
            run = control.load()
            if run["steps"].get(step, {}).get("status") == "completed":
                return public(run)
            if any(run["steps"].get(prior, {}).get("status") != "completed" for prior in STEPS[:index]):
                raise GuidedError(409, "Conclua as etapas anteriores primeiro.")
            started = time.monotonic()
            entry = dict(status="running", summary="Executando no Azure.", startedAt=now(),
                         services=STEP_DEFINITIONS[index][3], data={})
            run["steps"][step] = entry
            run.pop("error", None)
            run["updatedAt"] = now()
            control.save(run)
            try:
                control.check()
                entry["data"] = getattr(self, "_" + step)(run, control)
                control.check()
                entry.update(status="completed", summary=STEP_DEFINITIONS[index][1] + ": concluído.",
                             finishedAt=now(), durationMs=round((time.monotonic() - started) * 1000))
                run.update(stage=index + 1, updatedAt=now())
                control.save(run)
            except Exception as error:
                # Journal failures (including programming faults), but never manufacture success.
                logger.error("Guided phase failed: step=%s type=%s", step, type(error).__name__)
                entry.update(status="failed", summary="Etapa não concluída; tente novamente.",
                             finishedAt=now(), durationMs=round((time.monotonic() - started) * 1000))
                run.update(error="Não foi possível concluir a etapa no Azure.", updatedAt=now(),
                           stage=index)
                try:
                    control.save(run)
                except Exception as journal_error:
                    logger.error("Guided journal unavailable: %s", type(journal_error).__name__)
                if isinstance(error, GuidedError):
                    raise
                raise GuidedError(502, "Falha na etapa Azure; evidências e versões serão preservadas na retomada.") from error
            return public(run)

    def _source(self, run, control):
        raw = b"".join(build_record(row) for row in run["_sourceRows"])
        info = self.cloud.put(path(run, "source"), raw)
        self.cloud.put(path(run, "metadata"), encoded(run["_metadata"]), "application/json")
        records = [dict(accountId=meta["accountId"], eventId=meta["eventId"],
                        sourceVersion=meta["sourceVersion"], previousVersion=meta["previousVersion"],
                        hex=raw[index * self.size:(index + 1) * self.size].hex())
                   for index, meta in enumerate(run["_metadata"])]
        return dict(simulated=True, origin="SIMULADOR hospedado no serviço de computação Azure",
                    codePage="cp037", recordByteCount=self.size, byteCount=len(raw),
                    hex=raw.hex(), hex64=raw[:64].hex(), base64=base64.b64encode(raw).decode(),
                    sha256=digest(raw), records=records, accountIds=[r["accountId"] for r in records],
                    fields=[asdict(field) for field in self.fields],
                    copybookText=DEFAULT_COPYBOOK.read_text(encoding="utf-8"), blob=info,
                    download=self._artifact_link(run, "source"))

    def _transfer(self, run, control):
        raw = self.cloud.get(path(run, "source"))
        if digest(raw) != run["steps"]["source"]["data"]["sha256"]:
            raise GuidedError(502, "A integridade do blob de origem não confere.")
        info = self.cloud.put(path(run, "landing"), raw)
        received = self.cloud.get(path(run, "landing"))
        if received != raw:
            raise GuidedError(502, "A cópia recebida não corresponde aos bytes de origem.")
        return dict(sourceSha256=digest(raw), landingSha256=digest(received), sha256=digest(received),
                    byteCount=len(received), matches=True, parsingPerformed=False,
                    sourceBlob=self.cloud.info(path(run, "source")), landingBlob=info,
                    download=self._artifact_link(run, "landing"))

    def _landing(self, run):
        raw = self.cloud.get(path(run, "landing"))
        if (digest(raw) != run["steps"]["transfer"]["data"]["landingSha256"]
                or len(raw) != len(run["_metadata"]) * self.size):
            raise GuidedError(502, "A integridade ou o tamanho do arquivo recebido não confere.")
        return raw

    def _parse(self, run, control):
        raw = self._landing(run)
        records = []
        for index in range(len(raw) // self.size):
            image = raw[index * self.size:(index + 1) * self.size]
            row = parse_record(image, self.fields)
            fields = [dict(asdict(field), rawHex=image[field.offset:field.offset + field.length].hex(),
                           decodedValue=row[field.name]) for field in self.fields]
            records.append(dict(recordNumber=index + 1, accountId=row["account_id"],
                                row=row, fields=fields, document=normalize_document(row, index + 1)))
        self.cloud.put(path(run, "parsed"), encoded(records), "application/json")
        return dict(byteCount=len(raw), sha256=digest(raw), records=records,
                    fields=[asdict(field) for field in self.fields],
                    download=self._artifact_link(run, "parsed"), input="Bytes do blob landing")

    def _publish(self, run, control):
        raw = self._landing(run)
        events = []
        for index, meta in enumerate(run["_metadata"]):
            image = raw[index * self.size:(index + 1) * self.size]
            row = parse_record(image, self.fields)
            if row["account_id"] != meta["accountId"] or row["sequence_number"] != meta["sourceVersion"]:
                raise GuidedError(502, "Os bytes recebidos não correspondem ao envelope da origem.")
            event = make_event(row, meta["sourceVersion"], meta["previousVersion"],
                               event_id=meta["eventId"], committed_at=meta["committedAt"])
            event.update(recordBase64=base64.b64encode(image).decode(), runId=run["runId"], phase=run["phase"])
            events.append(event)
        self.cloud.put(path(run, "events"), encoded(events), "application/json")
        if "_publish" not in run:
            run["_publish"] = dict(startingPositions=self.cloud.starting_positions(),
                                   eventIds=[event["eventId"] for event in events], preparedAt=now())
            control.save(run)  # Before the first send, including sends that fail ambiguously.
        for event in events:
            control.check()
            self.cloud.send(event)
        return dict(**run["_publish"], events=events, count=len(events), partitionKey="accountId",
                    delivery="pelo menos uma vez", sinkAcknowledged=False,
                    download=self._artifact_link(run, "events"))

    def _apply(self, run, control):
        expected = {event["eventId"]: event for event in json.loads(self.cloud.get(path(run, "events")))}
        gate = threading.RLock()

        def done():
            with gate:
                return expected.keys() <= run["_received"].keys()

        def receive(partition, offset, sequence, body):
            with gate:
                control.check()
                try:
                    event = json.loads(body)
                except (ValueError, UnicodeError):
                    return  # Unrelated messages never advance this run's checkpoints.
                if (not isinstance(event, dict) or event.get("runId") != run["runId"]
                        or event.get("phase") != run["phase"]):
                    return
                event_id = event.get("eventId")
                if not isinstance(event_id, str) or event_id not in expected:
                    return
                if canonical(event) != canonical(expected[event_id]):
                    raise GuidedError(502, "Evento recebido diverge do envelope publicado.")
                if event_id in run["_received"]:
                    return
                result = self.engine.process(event, partition, offset)
                if not result.checkpoint_safe or result.outcome not in ("applied", "duplicate"):
                    raise GuidedError(502, "O destino não confirmou uma versão válida; verifique a quarentena.")
                run["_received"][event_id] = dict(event=event, partition=str(partition), offset=str(offset),
                                                  sequenceNumber=sequence, outcome=result.outcome,
                                                  sinkDurableAt=now())
                run["_checkpoints"][str(partition)] = dict(sequenceNumber=sequence, offset=str(offset),
                                                          eventId=event_id, savedAt=now())
                control.save(run)  # Cosmos is durable before this run-scoped Blob checkpoint.

        positions = dict(run["_publish"]["startingPositions"])
        positions.update({key: value["sequenceNumber"] for key, value in run["_checkpoints"].items()})
        self.cloud.consume(positions, receive, done)
        if not done():
            raise GuidedError(502, "Prazo de consumo esgotado; tente novamente. Mensagens podem ter expirado.")
        return dict(count=len(run["_received"]), received=list(run["_received"].values()),
                    checkpoints=run["_checkpoints"], checkpointScope="runId + phase + partition",
                    consumerGroup=self.cloud.group, transport="Mensagens reais recebidas do Azure Event Hubs",
                    download=self._artifact_link(run, "checkpoints"))

    def _verify(self, run, control):
        comparisons = []
        for account, row in run["_expected"].items():
            control.check()
            actual = self.cloud.store.get(account)
            expected = normalize_document(row, row["sequence_number"])
            expected.pop("sourceRecordNumber")
            expected.update(sourceVersion=row["sequence_number"], sourceEpoch=EPOCH, deleted=False)
            matches = {key: actual is not None and actual.get(key) == value for key, value in expected.items()}
            comparisons.append(dict(accountId=account, expected=expected, actual=actual,
                                    fields=matches, matches=all(matches.values())))
        evidence = dict(expectedCount=len(comparisons),
                        actualCount=sum(item["actual"] is not None for item in comparisons),
                        matches=all(item["matches"] for item in comparisons), records=comparisons,
                        verification="Leituras pontuais reais do Cosmos DB; expectativa independente dos bytes decodificados",
                        elapsedSinceCreationMs=round((datetime.now(timezone.utc) -
                            datetime.fromisoformat(run["createdAt"])).total_seconds() * 1000),
                        timingNote="Inclui pausas manuais; não é medição de SLA.")
        if not evidence["matches"]:
            run["steps"]["verify"]["data"] = evidence
            raise GuidedError(502, "A verificação encontrou diferenças no Cosmos DB.")
        return evidence

    def change(self, run_id):
        with self.cloud.lock(validate_run_id(run_id)) as control:
            run = control.load()
            if run["steps"].get("verify", {}).get("status") != "completed":
                # A repeated change click before source starts returns the already-created phase.
                if run["phase"] > 1 and run["stage"] == 0 and not run["steps"]:
                    return public(run)
                raise GuidedError(409, "Conclua a verificação antes de criar uma alteração.")
            run["history"].append(dict(phase=run["phase"], steps=copy.deepcopy(run["steps"])))
            row = dict(run["_expected"][run["_primaryAccount"]])
            row.update(current_balance=format(Decimal(row["current_balance"]) + Decimal("10.01"), ".2f"),
                       sequence_number=row["sequence_number"] + 1)
            run["_expected"][row["account_id"]] = row
            stamp = now()
            run.update(phase=run["phase"] + 1, stage=0, steps={}, artifacts={},
                       phaseCreatedAt=stamp, updatedAt=stamp)
            run.pop("error", None)
            self._prepare(run, [row], stamp)
            control.save(run)
            return public(run)

    def artifact(self, run_id, kind):
        run = self.cloud.load_manifest(validate_run_id(run_id))
        if kind not in {"source", "landing", "parsed", "events", "checkpoints"}:
            raise GuidedError(404, "Artefato desconhecido.")
        if kind == "checkpoints":
            if not run["_checkpoints"]:
                raise GuidedError(404, "Nenhum checkpoint durável nesta fase.")
            raw = encoded(dict(runId=run["runId"], phase=run["phase"], consumerGroup=self.cloud.group,
                               scope="runId + phase + partition", checkpoints=run["_checkpoints"]))
        else:
            raw = self.cloud.get(path(run, kind))
        binary = kind in {"source", "landing"}
        return raw, "application/octet-stream" if binary else "application/json", (
            f"{run['runId']}-phase-{run['phase']}-{kind}.{'bin' if binary else 'json'}")

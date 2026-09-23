from types import SimpleNamespace

from vsam_offload.guided_cloud import AzureCloud


def test_container_app_service_evidence_uses_actual_resource_provider():
    cloud = object.__new__(AzureCloud)
    cloud.env = {
        "AZURE_SUBSCRIPTION_ID": "synthetic-subscription",
        "AZURE_RESOURCE_GROUP": "synthetic-group",
        "AZURE_WEBAPP_NAME": "app-vsam-demo",
        "AZURE_COMPUTE_KIND": "containerapp",
        "AZURE_LOCATION": "brazilsouth",
        "STORAGE_ACCOUNT_URL": "https://synthetic.blob.core.windows.net",
        "COSMOS_ENDPOINT": "https://synthetic.documents.azure.com",
        "EVENTHUB_FULLY_QUALIFIED_NAMESPACE": "synthetic.servicebus.windows.net",
    }
    cloud.hub = "vsam-changes"
    cloud.group = "guided-demo"
    cloud.database_name = "synthetic-database"
    cloud.container_name = "balances"
    cloud.raw = SimpleNamespace(container_name="synthetic-raw")
    cloud.control = SimpleNamespace(container_name="demo-control")
    config = cloud.config()
    assert config["host"]["type"] == "Azure Container Apps"
    assert config["host"]["portalUrl"].endswith("/Microsoft.App/containerApps/app-vsam-demo")
    assert "App Service" not in str(config)
    services = {service["id"]: service for service in config["services"]}
    assert services["compute"]["details"] == {"region": "brazilsouth"}
    assert services["storage"]["details"] == {"account": "synthetic", "container": "synthetic-raw"}
    assert services["eventhubs"]["details"] == {
        "namespace": "synthetic", "eventHub": "vsam-changes", "consumerGroup": "guided-demo",
        "partitionKey": "accountId",
    }
    assert services["cosmos"]["details"] == {
        "account": "synthetic", "database": "synthetic-database", "container": "balances",
        "partitionKey": "/accountId",
    }
    assert services["control"]["details"] == {"account": "synthetic", "container": "demo-control"}

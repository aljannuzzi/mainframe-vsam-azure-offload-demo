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
    cloud.container_name = "balances"
    cloud.control = SimpleNamespace(container_name="demo-control")
    config = cloud.config()
    assert config["host"]["type"] == "Azure Container Apps"
    assert config["host"]["portalUrl"].endswith("/Microsoft.App/containerApps/app-vsam-demo")
    assert "App Service" not in str(config)

import pytest
from azure.cosmos import exceptions
from fastapi import HTTPException

from vsam_offload import api


class Container:
    def __init__(self, document=None, error=None):
        self.document = document
        self.error = error

    def read_item(self, *, item, partition_key):
        assert item == partition_key == "000000100001"
        if self.error:
            raise self.error
        return self.document


def test_api_hides_cosmos_system_fields(monkeypatch):
    document = {"id": "000000100001", "currentBalance": "12.30", "_etag": "internal"}
    monkeypatch.setattr(api, "get_container", lambda: Container(document=document))
    assert api.get_balance("000000100001") == {"id": "000000100001", "currentBalance": "12.30"}


def test_api_exposes_trace_metadata_not_internal_hash(monkeypatch):
    document = {"id": "000000100001", "_sync": {"sourcePosition": "synthetic:1", "eventHash": "private"}}
    monkeypatch.setattr(api, "get_container", lambda: Container(document=document))
    assert api.get_balance("000000100001")["synchronization"] == {"sourcePosition": "synthetic:1"}


@pytest.mark.parametrize("deleted", [True, False])
def test_missing_or_deleted_account_is_404(monkeypatch, deleted):
    container = Container(document={"deleted": True}) if deleted else Container(
        error=exceptions.CosmosResourceNotFoundError(status_code=404, message="missing")
    )
    monkeypatch.setattr(api, "get_container", lambda: container)
    with pytest.raises(HTTPException) as error:
        api.get_balance("000000100001")
    assert error.value.status_code == 404


def test_forbidden_is_not_a_missing_account(monkeypatch):
    forbidden = exceptions.CosmosHttpResponseError(status_code=403, message="network denied")
    monkeypatch.setattr(api, "get_container", lambda: Container(error=forbidden))
    with pytest.raises(exceptions.CosmosHttpResponseError) as error:
        api.get_balance("000000100001")
    assert error.value.status_code == 403

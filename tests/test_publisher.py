import json

import pytest

from vsam_offload.eventhub_publisher import publish


class Producer:
    def __init__(self):
        self.keys = []
        self.sent = []

    def create_batch(self, *, partition_key):
        self.keys.append(partition_key)
        return Batch()

    def send_batch(self, batch):
        self.sent.append(batch.events)


class Batch:
    def __init__(self):
        self.events = []

    def add(self, event):
        self.events.append(event)


def test_publisher_routes_repeated_key_to_same_partition_key():
    producer = Producer()
    events = [json.dumps({"accountId": key}) for key in ["0001", "0002", "0001"]]
    assert publish(producer, events) == 3
    assert producer.keys == ["0001", "0002", "0001"]


@pytest.mark.parametrize("body", ['{}', 'null', '{"accountId":""}', '{"accountId":12}'])
def test_missing_routing_key_is_not_silently_published(body):
    producer = Producer()
    with pytest.raises(ValueError):
        publish(producer, [body])
    assert not producer.sent

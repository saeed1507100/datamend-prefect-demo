from flows.orders import transform


def test_transform_plain_amounts():
    rows = [{"id": "1", "customer": "acme", "amount": "12.50", "currency": "USD"}]
    assert transform.fn(rows) == [(1, "acme", 12.5, "USD")]

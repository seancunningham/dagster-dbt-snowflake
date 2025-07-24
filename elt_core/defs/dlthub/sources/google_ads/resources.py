def get_campaigns():
    response = [
        {"id":10001, "name":"summer_sale", "start_date": "2024-06-01", "criteria": [{"id":1}, {"id":2}]},
        {"id":20002, "name":"winter_sale", "start_date": "2024-01-01", "criteria": [{"id":2}] }
    ]
    yield response

def get_criterion():
    response = [
        {"id":1, "type":"audience", "value":"summer_shoppers"},
        {"id":2, "type":"age", "value":"20-35"},
    ]
    yield response
    
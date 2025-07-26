from typing import Callable, Any
import requests

def get_exchange_rate(currency: str) -> Callable[[], Any]:

    uri = f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@2025-01-01/v1/currencies/{currency}.json"

    def exchange_api():# -> Any:
        response = requests.get(uri)
        yield response.json()
    
    return exchange_api

if __name__ == "__main__":
    print(get_exchange_rate("cad")().__next__())
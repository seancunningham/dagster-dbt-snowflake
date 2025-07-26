from typing import Callable, Any, Generator
import requests

def get_exchange_rate(currency: str) -> Callable[[], Any]:

    uri = ("https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api"
           "@latest"
            "/v1/"
            f"currencies/{currency}.json")

    def exchange_api() -> Generator[Any, Any, None]:# -> Any:
        response = requests.get(uri)
        yield response.json()
    
    return exchange_api

if __name__ == "__main__":
    print(get_exchange_rate("cad")().__next__())
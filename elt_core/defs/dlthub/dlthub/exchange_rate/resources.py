from typing import Callable, Any, Generator
import requests

def get_exchange_rate(currency: str) -> Callable[[], Any]:
    """
    Return a generator that will yield responses from an api
    with daily exchange rates for the selected currency
    
    :param currency: three character currency identifier
    :type currency: str
    :return: generator that yields the exchange rate
    :rtype: Callable[[], Any]
    """

    uri = ("https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api"
           "@latest"
            "/v1/"
            f"currencies/{currency}.json")

    def exchange_api() -> Generator[Any, Any, None]:# -> Any:
        response = requests.get(uri)
        yield response.json()
        while next_uri := response.json().get("next_page"):
            response = requests.get(next_uri)
            yield response.json()

    return exchange_api

if __name__ == "__main__":
    get_ex = get_exchange_rate("usd")
    for x in get_ex():
        print(x)
from typing import Sequence
from importlib import import_module
import os

from dagster_sling import SlingConnectionResource

def load_objects(path: str, object_types: Sequence[type]) -> Sequence[object]:
    paths = []
    objects = []
    for child in os.listdir(path):
        paths.append(f"{path}/{child}")
    
    while paths:
        path = paths.pop()
        if os.path.isfile(path):
            if path.endswith(".py"):
                module_name = path[:-3].replace("/", ".")
                module = import_module(module_name)
                valid_objects = [v for k, v in module.__dict__.items() if not k.startswith("__") and type(v) in object_types]
                objects.extend(valid_objects)
        else:
            for child in os.listdir(path):
                paths.append(f"{path}/{child}")
    
    return objects

def load_sling_connection_resources(path: str) -> Sequence[SlingConnectionResource]:
    return load_objects(path, [SlingConnectionResource])
import hashlib
import json
import os
import socket

import yaml


def get_host_name():
    return socket.gethostname()


def read_yaml(path):
    # if not exisit create
    if not os.path.exists(path):
        empty_yaml = {".": "."}
        if not os.path.isdir(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path), exist_ok=True)
        write_yaml(path, empty_yaml)

    with open(path) as stream:
        read = yaml.load(stream, Loader=yaml.UnsafeLoader)

    return read


def write_yaml(path, dict_file):
    with open(path, "w") as f:
        data = yaml.dump(dict_file, f, default_flow_style=False, sort_keys=False)


def read_key_from_yaml(key, path):
    yaml_file = read_yaml(path)

    if key in yaml_file:
        return yaml_file[key]
    else:
        return None


def hash_dict(dict_to_hash: dict) -> str:
    dhash = hashlib.md5()
    encoded = json.dumps(dict_to_hash, sort_keys=True, default=str).encode()
    dhash.update(encoded)
    return dhash.hexdigest()

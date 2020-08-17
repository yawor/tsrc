""" Parse tsrc config files """

from path import Path
import ruamel.yaml
from schema import Schema, SchemaError
from typing import Any, Dict, NewType

import tsrc

Config = NewType("Config", Dict[str, Any])


def parse_config(file_path: Path, *, schema: Schema) -> Config:
    try:
        contents = file_path.read_text()
    except OSError as os_error:
        raise tsrc.InvalidConfig(file_path, os_error)
    try:
        yaml = ruamel.yaml.YAML(typ="safe", pure=True)
        parsed = yaml.load(contents)
    except ruamel.yaml.error.YAMLError as yaml_error:
        raise tsrc.InvalidConfig(file_path, yaml_error)
    try:
        schema.validate(parsed)
    except SchemaError as schema_error:
        raise tsrc.InvalidConfig(file_path, schema_error)
    return Config(parsed)

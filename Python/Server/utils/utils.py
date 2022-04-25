import json
import logging.config
import os.path
import sys
from pathlib import Path
from typing import Dict, Any, NoReturn

root_path: str = str(Path(__file__).parent.parent)


def read_json_configs(path: str) -> Dict[str, Any]:
    with open(path) as f:
        return json.load(f)


logging_configs = read_json_configs(os.path.join(root_path, "configs/logger-configs.json"))["logging"]
logging_configs["handlers"]["console"]["stream"] = sys.stdout
logging.config.dictConfig(logging_configs)


def info(msg: str) -> NoReturn:
    logging.info(msg)


def warn(msg: str) -> NoReturn:
    logging.warning(msg)


def error(msg: str) -> NoReturn:
    logging.error(msg)

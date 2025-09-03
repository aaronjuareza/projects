# etl/common/config.py
import os
from pathlib import Path
import yaml

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]

def load_config():
    load_dotenv(ROOT / ".env")
    with open(ROOT / "config.yml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    # Resuelve creds desde variables de entorno
    for side in ("source", "target"):
        m = cfg[side]["mysql"]
        m["user"] = os.getenv(m.pop("user_env"))
        m["password"] = os.getenv(m.pop("pass_env"))
    return cfg

# etl/common/state.py
import json
from pathlib import Path
from datetime import datetime

STATE_PATH = Path(__file__).resolve().parents[1] / "state.json"

def read_state():
    if not STATE_PATH.exists():
        return {}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))

def write_state(data: dict):
    data["last_run_utc"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    STATE_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")

import json
from pathlib import Path

_CONFIG_DIR = Path.home() / '.ootp_analyzer'
_CONFIG_FILE = _CONFIG_DIR / 'config.json'

_DEFAULTS = {
    'league_file': '',
    'min_pa': 50,
    'min_ip': 20,
    'prospect_max_age': 25,
}


def load() -> dict:
    if _CONFIG_FILE.exists():
        try:
            data = json.loads(_CONFIG_FILE.read_text())
            return {**_DEFAULTS, **data}
        except Exception:
            pass
    return _DEFAULTS.copy()


def save(cfg: dict) -> None:
    _CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    _CONFIG_FILE.write_text(json.dumps(cfg, indent=2))

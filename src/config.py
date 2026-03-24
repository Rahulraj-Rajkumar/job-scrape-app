from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


def load_config(config_path: str = "config.yaml") -> dict[str, Any]:
    load_dotenv()

    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    if not isinstance(config, dict):
        raise ValueError(f"Invalid config: expected mapping at {config_path}")
    config.setdefault("email", {})

    # Override email settings from environment
    env_overrides = {
        "sender": os.getenv("EMAIL_SENDER"),
        "recipient": os.getenv("EMAIL_RECIPIENT"),
        "app_password": os.getenv("EMAIL_APP_PASSWORD"),
    }
    for key, val in env_overrides.items():
        if val:
            config["email"][key] = val

    if os.getenv("SENDGRID_API_KEY"):
        config["email"]["sendgrid_api_key"] = os.getenv("SENDGRID_API_KEY")

    return config


def load_ats_slugs(slugs_path: str = "ats_slugs.yaml") -> dict[str, Any]:
    path = Path(slugs_path)
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def save_ats_slugs(slugs: dict[str, Any], slugs_path: str = "ats_slugs.yaml") -> None:
    with open(slugs_path, "w", encoding="utf-8") as f:
        yaml.dump(slugs, f, default_flow_style=False, sort_keys=False)

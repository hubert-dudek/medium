# Databricks notebook source
import os
from dataclasses import asdict, dataclass

def read_boolean(name, default="false"):
    value = os.getenv(name, default).strip().lower()
    if value not in ("true", "false"):
        raise ValueError(f"{name} must be true or false")
    return value == "true"

@dataclass(frozen=True)
class Settings:
    app_env: str
    batch_size: int
    enable_export: bool

settings = Settings(
    app_env=os.environ["APP_ENV"],
    batch_size=int(os.getenv("BATCH_SIZE", "500")),
    enable_export=read_boolean("ENABLE_EXPORT"),
)
if settings.batch_size <= 0:
    raise ValueError("BATCH_SIZE must be greater than zero")

print(settings)
print(f"Batch size type: {type(settings.batch_size).__name__}")
print(f"Export enabled type: {type(settings.enable_export).__name__}")

# COMMAND ----------
import json

dbutils.notebook.exit(json.dumps(asdict(settings)))

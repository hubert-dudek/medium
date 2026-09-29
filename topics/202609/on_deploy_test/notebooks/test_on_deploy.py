# Databricks notebook source
from datetime import datetime, timezone

print("SUCCESS: the on-deploy test notebook ran.")
print(f"Run time (UTC): {datetime.now(timezone.utc).isoformat()}")

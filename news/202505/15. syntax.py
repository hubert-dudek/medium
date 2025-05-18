# Databricks notebook source
# MAGIC %md
# MAGIC 16.4 LTS

# COMMAND ----------

# main.py

import requests
from typing import List


def fetch_data(url: str) -> dict:
    """Fetch JSON data from a given URL."""
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def process_items(items: List[dict]) -> List[str]:
    """Extract 'name' field from a list of dictionaries."""
    names = [item["name"] for item in items if "name" in item]
    return sorted(names)


def main():
    url = "https://jsonplaceholder.typicode.com/users"
    try:
        data = fetch_data(url)
        names = process_items(data)
        for name in names:
            print(f"User: {name}")
    except requests.HTTPError as e:
        print(f"HTTP Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()


"""Ordinary Python module: store application settings in one place.

This module does not load a .env file. Databricks supplies the process values.
These assignments run when the module is first imported in a Python process.
"""
import os

APP_ENV = os.environ["APP_ENV"]
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
FILE_ONLY = os.getenv("FILE_ONLY")

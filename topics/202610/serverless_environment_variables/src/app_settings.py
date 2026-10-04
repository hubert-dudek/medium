"""Read configuration at import time, just like an ordinary Python package."""
import os

APP_ENV = os.environ["APP_ENV"]
LOG_LEVEL = os.environ["LOG_LEVEL"]
FILE_ONLY = os.environ["FILE_ONLY"]

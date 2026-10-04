import os

APP_ENV = os.environ["APP_ENV"]
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
FILE_ONLY = os.getenv("FILE_ONLY")

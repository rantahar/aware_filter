#!/usr/bin/env python3
"""WSGI entry point for AWARE Filter service"""

from aware_filter import app
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    app.run()
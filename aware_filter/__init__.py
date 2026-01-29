#!/usr/bin/env python3
"""
AWARE Webservice Receiver
Receives JSON POST data from AWARE clients and writes to MySQL database.
No filtering yet - just direct passthrough.
"""

from .utils import logger


def main():
    """Entry point placeholder. Flask endpoints are disabled by default.

    To run the HTTP endpoints explicitly, call:
        from aware_filter import flask_endpoints
        flask_endpoints.run_server()
    """
    logger.info('Flask endpoints are disabled by default. To start them, call aware_filter.flask_endpoints.run_server()')


if __name__ == '__main__':
    main()


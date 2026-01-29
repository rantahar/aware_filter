#!/usr/bin/env python3
"""Utility helpers for aware_filter not tied to HTTP or DB specifics."""

from dotenv import load_dotenv
import os
import logging
import psutil
import gc

load_dotenv()

# Configure package logging early
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Package-level constants
JOIN_STUDY_PASSWORD = "tokenwithnospecialcharatctersbutseveralfiretypepokemon"
STUDY_ID = "Polalpha"
CONFIG_FILE_PATH = os.getenv('CONFIG_FILE_PATH', 'aware_config.json')


def check_memory_usage():
    """Check current memory usage and log warnings if high"""
    process = psutil.Process()
    memory_info = process.memory_info()
    memory_mb = memory_info.rss / 1024 / 1024

    if memory_mb > 400:  # Warning if over 400MB
        logger.warning(f"High memory usage: {memory_mb:.1f} MB")
        if memory_mb > 500:  # Force garbage collection if over 500MB
            gc.collect()
            logger.info("Forced garbage collection due to high memory usage")

    return memory_mb


# Shared mutable stats dictionary (tests may supply their own)
stats = {
    'total_requests': 0,
    'successful_inserts': 0,
    'failed_inserts': 0,
    'unauthorized_attempts': 0
}

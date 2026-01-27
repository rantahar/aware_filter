"""Shared test fixtures and configuration"""

import pytest
import sys
from pathlib import Path

# Add the parent directory to the path so imports work
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def stats():
    """Fixture providing a fresh stats dictionary"""
    return {
        'total_requests': 0,
        'successful_inserts': 0,
        'failed_inserts': 0,
        'unauthorized_attempts': 0
    }


@pytest.fixture
def sample_data():
    """Fixture providing sample sensor data"""
    return {
        'device_id': '123',
        'timestamp': '2024-01-27T10:30:00',
        'value': 42.5,
        'unit': 'celsius'
    }


@pytest.fixture
def sample_data_list():
    """Fixture providing a list of sample sensor data"""
    return [
        {'device_id': '123', 'timestamp': '2024-01-27T10:30:00', 'value': 42.5},
        {'device_id': '124', 'timestamp': '2024-01-27T10:31:00', 'value': 43.1},
        {'device_id': '125', 'timestamp': '2024-01-27T10:32:00', 'value': 41.8},
    ]

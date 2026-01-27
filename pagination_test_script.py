#!/usr/bin/env python3
"""
Test script for pagination functionality
Usage: python test_pagination.py
"""

import requests
import json
import sys

# Configuration
BASE_URL = "https://localhost:3446"
USERNAME = "admin"  # Replace with actual username
PASSWORD = "password"  # Replace with actual password

def get_auth_token():
    """Get authentication token"""
    try:
        response = requests.post(
            f"{BASE_URL}/login",
            json={"username": USERNAME, "password": PASSWORD},
            verify=False  # Self-signed certificate
        )
        if response.status_code == 200:
            return response.json().get("token")
        else:
            print(f"Login failed: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Login error: {e}")
        return None

def run_pagination_tests(token):
    """Test pagination with different parameters"""
    
    headers = {"Authorization": f"Bearer {token}"}
    
    test_cases = [
        {
            "name": "Default pagination (no limit)",
            "params": {"table": "accelerometer_transformed", "device_uid": "2"}
        },
        {
            "name": "Small limit test",
            "params": {"table": "accelerometer_transformed", "device_uid": "2", "limit": "100"}
        },
        {
            "name": "Pagination with offset",
            "params": {"table": "accelerometer_transformed", "device_uid": "2", "limit": "100", "offset": "100"}
        },
        {
            "name": "Large limit test",
            "params": {"table": "accelerometer_transformed", "device_uid": "2", "limit": "1000"}
        },
        {
            "name": "Invalid limit (too high)",
            "params": {"table": "accelerometer_transformed", "device_uid": "2", "limit": "100000"}
        },
        {
            "name": "Invalid limit (negative)",
            "params": {"table": "accelerometer_transformed", "device_uid": "2", "limit": "-10"}
        },
        {
            "name": "Invalid offset (negative)",
            "params": {"table": "accelerometer_transformed", "device_uid": "2", "offset": "-5"}
        }
    ]
    
    for test_case in test_cases:
        print(f"\n🧪 Testing: {test_case['name']}")
        print("Parameters:", test_case['params'])
        
        try:
            response = requests.get(
                f"{BASE_URL}/data",
                params=test_case['params'],
                headers=headers,
                verify=False,
                timeout=30
            )
            
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"Records returned: {data.get('count', 'N/A')}")
                print(f"Total records: {data.get('total_count', 'N/A')}")
                print(f"Has more: {data.get('has_more', 'N/A')}")
                print(f"Limit: {data.get('limit', 'N/A')}")
                print(f"Offset: {data.get('offset', 'N/A')}")
                
                if 'warning' in data:
                    print(f"⚠️ Warning: {data['warning']}")
                    
                print("✅ Success")
            else:
                print(f"❌ Error: {response.text}")
                
        except requests.exceptions.Timeout:
            print("❌ Request timed out (this should not happen with pagination)")
        except Exception as e:
            print(f"❌ Request failed: {e}")

def main():
    print("🚀 Testing AWARE Filter Pagination")
    print("=" * 50)
    
    # Get authentication token
    token = get_auth_token()
    if not token:
        print("❌ Failed to authenticate")
        sys.exit(1)
    
    print("✅ Authentication successful")
    
    # Run pagination tests
    run_pagination_tests(token)
    
    print("\n" + "=" * 50)
    print("🏁 Pagination tests completed")

if __name__ == "__main__":
    main()
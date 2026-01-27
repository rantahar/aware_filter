# Pagination Guide for AWARE Filter API

## Overview

To prevent memory exhaustion and timeouts when querying large datasets, the AWARE Filter API now implements pagination with configurable limits.

## Key Changes

### Default Behavior
- **Default limit**: 10,000 records per request
- **Maximum limit**: 50,000 records per request
- Large datasets now return pagination metadata

### New Query Parameters

#### `limit` (optional)
- Controls the maximum number of records returned
- Must be a positive integer
- Cannot exceed 50,000
- Default: 10,000

#### `offset` (optional)
- Number of records to skip from the beginning
- Must be a non-negative integer
- Default: 0
- Use with `limit` to paginate through large datasets

### Example Usage

#### Basic pagination (first 1000 records)
```
GET /data?table=accelerometer_transformed&device_uid=2&limit=1000
```

#### Get next page (records 1001-2000)
```
GET /data?table=accelerometer_transformed&device_uid=2&limit=1000&offset=1000
```

#### Query with time range and pagination
```
GET /data?table=accelerometer_transformed&device_uid=2&start_time=1600000000&end_time=1600086400&limit=5000&offset=10000
```

### Response Format

```json
{
  "data": [...],
  "count": 1000,
  "total_count": 9915712,
  "limit": 1000,
  "offset": 0,
  "has_more": true,
  "warning": "Large dataset (9915712 total records). Consider using pagination with limit and offset parameters."
}
```

### Response Fields

- `data`: Array of records returned
- `count`: Number of records in current response
- `total_count`: Total number of records matching the query
- `limit`: Applied limit value
- `offset`: Applied offset value
- `has_more`: Boolean indicating if more records are available
- `warning`: Displayed for datasets with >100,000 total records

## Best Practices

1. **Use appropriate limits**: Start with smaller limits (1000-5000) for initial requests
2. **Implement pagination**: Use `offset` to iterate through large datasets
3. **Monitor response size**: Check `total_count` to understand the full dataset size
4. **Time range filtering**: Use `start_time` and `end_time` to reduce query scope
5. **Progressive loading**: Fetch data in chunks to avoid timeouts

## Error Handling

The API will return appropriate error messages for:
- Invalid limit values (non-positive integers)
- Limit exceeding maximum (50,000)
- Invalid offset values (negative integers)

## Memory and Performance Improvements

- Gunicorn workers now recycle more frequently (every 500 requests)
- Timeout increased to 180 seconds for large queries
- Memory monitoring with automatic garbage collection
- Warning logs for large dataset operations
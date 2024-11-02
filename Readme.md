# DaftIcebergHandler

A flexible and robust Python handler for Apache Iceberg tables using Daft DataFrame operations and PostgreSQL catalog support.

## Overview

DaftIcebergHandler provides a high-performance interface for managing Apache Iceberg tables with support for:

- Table creation and management
- Efficient data ingestion
- Deduplication strategies
- Soft and hard delete capabilities
- Schema evolution
- Snapshot management
- Flexible read operations with filtering

## Features

- **Cloud Storage Support**: Compatible with various storage backends (GCS, S3, etc.)
- **PostgreSQL Catalog Integration**: Built-in support for PostgreSQL-based Iceberg catalogs
- **Flexible Schema Handling**: Support for custom schemas and schema evolution
- **Advanced Deduplication**: Multiple strategies for handling duplicate records
- **Comprehensive Logging**: Built-in logging with customization options
- **Error Handling**: Robust error handling and reporting
- **Type Safety**: Full typing support with Python type hints

## Installation

```bash
pip install daft pyiceberg psycopg2-binary pyarrow
```

## Quick Start

### 1. Define Your Schema

```python
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestampType, LongType

schema = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "name", StringType()),
    NestedField(3, "timestamp", TimestampType(), required=True),
    NestedField(4, "deleted", BooleanType(), required=False)
)
```

### 2. Initialize the Handler

```python
from daft_iceberg_handler import DaftIcebergHandler

handler = DaftIcebergHandler(
    warehouse_path="s3://your-bucket/warehouse",
    catalog_name="your_catalog",
    namespace="your_namespace",
    table_name="your_table",
    postgres_uri="postgresql+psycopg2://user:pass@host:5432/db",
    schema=schema
)
```

### 3. Basic Operations

```python
# Write data
data = [
    {"id": 1, "name": "Example", "timestamp": datetime.now(timezone.utc)},
    {"id": 2, "name": "Test", "timestamp": datetime.now(timezone.utc)}
]
handler.write_data(data)

# Read data
df = handler.read_data()

# Filter data
df = handler.read_data(filter_expression="name = 'Example'")

# Deduplicate
handler.deduplicate_table(column_name="id", keep="first")

# Soft delete
handler.soft_delete_data("id = 1")
```

## Advanced Usage

### Custom Record Preparation

```python
def prepare_record(record):
    """Custom record preparation logic."""
    return {
        "id": record["id"],
        "name": record["name"].strip().lower(),
        "timestamp": datetime.now(timezone.utc)
    }

handler.write_data(data, prepare_record_func=prepare_record)
```

### Table Management

```python
# Create new table
handler.manage_table("create", new_name="new_table", schema=custom_schema)

# Rename table
handler.manage_table("rename", old_name="old_table", new_name="new_table")

# Delete table
handler.manage_table("delete", old_name="table_to_delete")
```

### Snapshot Management

```python
# Read from specific snapshot
df = handler.read_data(snapshot_id=123)
```

## Best Practices

1. **Error Handling**

   ```python
   try:
       handler.write_data(data)
   except Exception as e:
       logger.error(f"Failed to write data: {e}")
       # Handle error appropriately
   ```
2. **Logging Configuration**

   ```python
   import logging
   logging.basicConfig(level=logging.INFO)
   custom_logger = logging.getLogger("iceberg_operations")
   handler = DaftIcebergHandler(..., logger=custom_logger)
   ```
3. **Deduplication Strategy**

   - Use `deduplicate_table()` with appropriate keep strategy
   - Consider impact on downstream processes
   - Monitor performance with large datasets
4. **Soft Delete Usage**

   - Prefer soft deletes over hard deletes
   - Implement periodic cleanup of soft-deleted records
   - Use `include_deleted` parameter in read operations

## Deduplication Methods

### `deduplicate_table`

The `deduplicate_table` method removes duplicate records from the table based on a specified column.

- **Arguments**:
  - `column_name` (str): The column to deduplicate on.
  - `include_deleted` (bool): Whether to include records marked as deleted.
  - `keep` (str): Whether to keep the 'first' or 'last' occurrence of each duplicate.

Example usage:

```python
handler.deduplicate_table(include_deleted=True)
```

### `deduplicate_table_with_order`

The `deduplicate_table_with_order` method allows deduplication while maintaining a specific sort order,
keeping either the 'first' or 'last' occurrence based on the sorting.

- **Arguments**:
  - `column_name` (str): The column to deduplicate on.
  - `include_deleted` (bool): Whether to include records marked as deleted.
  - `keep` (str): Which occurrence to keep ('first' or 'last').

Example usage:

```python
handler.deduplicate_table_with_order(column_name='column_name', include_deleted=True, keep='first')
```

## Performance Considerations

- Use appropriate partitioning for large datasets
- Monitor memory usage during large write operations
- Consider batch size for bulk operations
- Use filter pushdown when possible

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## Support

For issues and feature requests, please [create an issue](link-to-your-repo/issues).

import logging
import hashlib
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import daft
import pyarrow as pa
from pyiceberg.table import Table
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, TimestampType, BooleanType
from pyiceberg.catalog import load_catalog
from pyiceberg.transforms import IdentityTransform

class DaftIcebergHandler:
    """A generic handler for Apache Iceberg tables using Daft DataFrame operations."""
    
    def __init__(self, 
                 warehouse_path: str,
                 catalog_name: str,
                 namespace: str,
                 table_name: str,
                 postgres_uri: str,
                 schema: Schema,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize the DaftIcebergHandler.
        
        Args:
            warehouse_path: Path to the warehouse (e.g., 's3://bucket/warehouse' or 'gs://bucket/warehouse')
            catalog_name: Name of the Iceberg catalog
            namespace: Namespace/database name
            table_name: Name of the table
            postgres_uri: PostgreSQL connection URI
            schema: PyIceberg Schema object defining the table structure
            logger: Optional custom logger instance
        """
        self.warehouse_path = warehouse_path
        self.catalog_name = catalog_name
        self.namespace = namespace
        self.table_name = table_name
        self.postgres_uri = postgres_uri
        self.initial_schema = schema
        self.logger = logger or logging.getLogger(__name__)
        self.catalog = self._initialize_catalog()
        self.table = self._create_or_load_table()

    def _initialize_catalog(self) -> Any:
        """Initialize the Iceberg catalog."""
        try:
            catalog = load_catalog(
                self.catalog_name,
                **{
                    "type": "sql",
                    "catalog-name": self.catalog_name,
                    "uri": self.postgres_uri,
                    "warehouse": self.warehouse_path,
                }
            )
            return catalog
        except Exception as e:
            self.logger.error(f"Failed to initialize catalog: {e}")
            raise

    def _create_or_load_table(self) -> Table:
        """Create or load the Iceberg table."""
        try:
            table_identifier = f"{self.namespace}.{self.table_name}"
            if not self.catalog.table_exists(table_identifier):
                self.logger.info(f"Creating table {table_identifier}...")
                self.catalog.create_table(
                    table_identifier,
                    schema=self.initial_schema,
                    properties={
                        "write.parquet.compression-codec": "snappy",
                        "write.metadata.compression-codec": "gzip",
                        "write.timestamp.precision": "us",
                        "format-version": "2"
                    }
                )
            return self.catalog.load_table(table_identifier)
        except Exception as e:
            self.logger.error(f"Failed to create/load table: {e}")
            raise

    def write_data(self, data: List[Dict[str, Any]], prepare_record_func: Optional[callable] = None) -> None:
        """
        Write data to the table.
        
        Args:
            data: List of dictionaries containing the data to write
            prepare_record_func: Optional function to prepare/transform records before writing
        """
        if self.table is None:
            self.logger.error("Cannot write data: Table is not loaded or has been deleted.")
            return

        try:
            if prepare_record_func:
                prepared_data = [prepare_record_func(record) for record in data]
            else:
                prepared_data = data

            if prepared_data:
                df = daft.from_pylist(prepared_data)
                df.write_iceberg(self.table)
                self.logger.info(f"Successfully wrote {len(prepared_data)} records")
            else:
                self.logger.info("No records to write.")

        except Exception as e:
            self.logger.error(f"Failed to write to table: {e}")
            raise

    def read_data(self, 
                  snapshot_id: Optional[int] = None, 
                  include_deleted: bool = False, 
                  filter_expression: Optional[str] = None) -> daft.DataFrame:
        """
        Read data from the table.
        
        Args:
            snapshot_id: Optional snapshot ID to read from
            include_deleted: Whether to include soft-deleted records
            filter_expression: Optional filter expression
            
        Returns:
            Daft DataFrame containing the requested data
        """
        if self.table is None:
            self.logger.warning("Cannot read data: Table is not loaded.")
            return None
        
        try:
            df = daft.read_iceberg(self.table, snapshot_id=snapshot_id)

            if not include_deleted and "deleted" in df.column_names:
                df = df.filter((df["deleted"] == False) | df["deleted"].is_null())

            if filter_expression:
                df = df.filter(filter_expression)

            return df
        except Exception as e:
            self.logger.error(f"Failed to read table: {e}")
            raise

    def deduplicate_table(self, include_deleted: bool = True):
        """Deduplicates the table based on the specified column, keeping the first occurrence."""
        try:
            df = self.read_data(include_deleted=include_deleted)
            # Daft deduplication (using distinct):
            df = df.distinct()  # Deduplicate using Daft's distinct

            df.write_iceberg(self.table, mode='overwrite')

            self.logger.info(f"Table deduplicated.")
            return df

        except Exception as e:
            self.logger.error(f"Error deduplicating table: {e}")
            raise

    def deduplicate_table_with_order(self, column_name: str = "column_name", include_deleted: bool = True, keep='first'):
        """Deduplicates the table, keeping the first or last occurrence based on sorting."""
        try:
            df = self.read_data(include_deleted=include_deleted)

            if keep not in ('first', 'last'):
                raise ValueError("keep must be either 'first' or 'last'")
            
            # Add a row number column for sorting (if not already there)
            if "__row_num__" not in df.column_names:
                df = df.with_column("__row_num__", daft.udf(lambda x: x, int)(daft.Series.range(0, df.size())))

            # Sort by the specified column and the row number. Add other sort columns if needed
            if keep == 'first':
                df = df.sort([column_name, "__row_num__"])
            else: # keep == 'last'
                df = df.sort([column_name, "__row_num__"], desc=[False, True]) # Descending order for row number

            # Group by the deduplication column
            df = df.groupby(column_name)

            # Aggregate to keep only the first or last row of each group
            df = df.agg(
                [
                    (col, "first") for col in df.column_names if col != column_name  # Keep the first value of other cols
                    # Add more aggregation logic as needed for specific columns (e.g., sum, max, etc.)
                ]
            )
            df = df.exclude("__row_num__") # Remove the row_num column

            df.write_iceberg(self.table, overwrite=True)
            self.logger.info(f"Table deduplicated based on {column_name}, keeping {keep}.")

        except Exception as e:
            self.logger.error(f"Error deduplicating table: {e}")
            raise

    def soft_delete_data(self, filter_expression: str) -> None:
        """
        Soft delete records matching the filter expression.
        
        Args:
            filter_expression: Expression to identify records to delete
        """
        try:
            self.table.update(
                [("deleted", True)],
                where=filter_expression
            )
            self.logger.info(f"Data soft deleted based on filter: {filter_expression}")
        except Exception as e:
            self.logger.error(f"Failed to soft delete data: {e}")
            raise

    def delete_data(self, filter_expression: str) -> None:
        """
        Hard delete records matching the filter expression.
        
        Args:
            filter_expression: Expression to identify records to delete
        """
        try:
            self.table.delete(where=filter_expression)
            self.logger.info(f"Data deleted based on filter: {filter_expression}")
        except Exception as e:
            self.logger.error(f"Failed to delete data: {e}")
            raise

    def manage_table(self, operation: str, old_name: str = None, new_name: str = None,
                    schema: Schema = None, properties: dict = None) -> None:
        """
        Manage table operations (create, delete, rename).
        
        Args:
            operation: Operation to perform ('create', 'delete', 'rename')
            old_name: Original table name (for rename operation)
            new_name: New table name (for create/rename operations)
            schema: Schema for new table
            properties: Table properties
        """
        try:
            if operation == 'create':
                table_identifier = f"{self.namespace}.{new_name}"
                if self.catalog.table_exists(table_identifier):
                    raise ValueError(f"Table {new_name} already exists")
                self.catalog.create_table(table_identifier, schema or self.initial_schema, properties)
                
            elif operation == 'delete':
                table_identifier = f"{self.namespace}.{old_name}"
                self.catalog.drop_table(table_identifier)
                if old_name == self.table_name:
                    self.table = None
                    
            elif operation == 'rename':
                old_identifier = f"{self.namespace}.{old_name}"
                new_identifier = f"{self.namespace}.{new_name}"
                self.catalog.rename_table(old_identifier, new_identifier)
                
            else:
                raise ValueError(f"Unsupported operation: {operation}")
                
            self.logger.info(f"Table operation {operation} completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in table operation {operation}: {e}")
            raise

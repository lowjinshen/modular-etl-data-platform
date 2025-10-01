"""
Utility - Schema Loader

This module provides functions to load and parse JSON schema definitions for the
configuration-driven ETL framework. It converts JSON schemas into PySpark StructType
objects and extracts metadata needed for data processing.
"""

import json
from typing import Dict, List, Optional, Any
from pathlib import Path
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType,
    DecimalType, DateType, TimestampType, BooleanType, DataType
)


class SchemaLoader:
    """
    This class provides methods to:
    - Load schema JSON files
    - Build PySpark StructType schemas
    - Extract primary keys, partition columns, and other metadata
    """
    
    # Mapping of string data types to PySpark types
    DATATYPE_MAPPING = {
        'string': StringType(),
        'long': LongType(),
        'integer': IntegerType(),
        'date': DateType(),
        'timestamp': TimestampType(),
        'boolean': BooleanType()
    }
    
    @staticmethod
    def load_schema(schema_path: str) -> Dict[str, Any]:
        """
        Load a JSON schema file from the specified path.
        
        Args:
            schema_path: Path to the JSON schema file (relative or absolute)
            
        Returns:
            Dictionary containing the complete schema definition
            
        Example:
            >>> schema = SchemaLoader.load_schema('config/bronze/schema/orders_schema.json')
            >>> print(schema['table_name'])
            'bronze_orders'
        """
        path = Path(schema_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_path}")
        
        try:
            with open(path, 'r') as f:
                schema_dict = json.load(f)
            return schema_dict
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in schema file {schema_path}: {str(e)}")
    
    @staticmethod
    def _parse_datatype(datatype_str: str) -> DataType:
        """
        Parse a datatype string into a PySpark DataType object.
        
        Handles simple types (string, long, integer, etc.) and complex types
        like decimal(10,2) that include precision and scale.
        
        Args:
            datatype_str: String representation of the datatype
            
        Returns:
            PySpark DataType object
            
        Example:
            >>> SchemaLoader._parse_datatype('string')
            StringType()
            >>> SchemaLoader._parse_datatype('decimal(10,2)')
            DecimalType(10,2)
        """
        datatype_str = datatype_str.strip().lower()
        
        # Handle decimal types with precision and scale
        if datatype_str.startswith('decimal'):
            # Extract precision and scale from format: decimal(10,2)
            import re
            match = re.match(r'decimal\((\d+),(\d+)\)', datatype_str)
            if match:
                precision = int(match.group(1))
                scale = int(match.group(2))
                return DecimalType(precision, scale)
            else:
                raise ValueError(f"Invalid decimal format: {datatype_str}. Expected format: decimal(p,s)")
        
        # Handle simple types
        if datatype_str in SchemaLoader.DATATYPE_MAPPING:
            return SchemaLoader.DATATYPE_MAPPING[datatype_str]
        
        raise ValueError(f"Unsupported datatype: {datatype_str}")
    
    @staticmethod
    def build_spark_schema(columns: List[Dict[str, Any]]) -> StructType:
        """
        Build a PySpark StructType schema from column definitions.
        
        Args:
            columns: List of column dictionaries with 'name', 'datatype', and 'nullable' keys
            
        Returns:
            PySpark StructType representing the schema
            
        Example:
            >>> columns = [
            ...     {"name": "order_id", "datatype": "long", "nullable": False},
            ...     {"name": "order_total", "datatype": "decimal(10,2)", "nullable": True}
            ... ]
            >>> schema = SchemaLoader.build_spark_schema(columns)
            >>> print(schema)
        """
        struct_fields = []
        
        for col in columns:
            field_name = col['name']
            datatype = SchemaLoader._parse_datatype(col['datatype'])
            nullable = col.get('nullable', True)
            
            struct_field = StructField(field_name, datatype, nullable)
            struct_fields.append(struct_field)
        
        return StructType(struct_fields)
    
    @staticmethod
    def build_full_spark_schema(schema_dict: Dict[str, Any], 
                                include_metadata: bool = True) -> StructType:
        """
        Build complete PySpark schema including both data and metadata columns.
        
        Args:
            schema_dict: Full schema dictionary from load_schema()
            include_metadata: Whether to include metadata_columns in the schema
            
        Returns:
            PySpark StructType with all columns
            
        Example:
            >>> schema = SchemaLoader.load_schema('config/bronze/orders_schema.json')
            >>> full_schema = SchemaLoader.build_full_spark_schema(schema)
        """
        # Build schema from data columns
        spark_schema = SchemaLoader.build_spark_schema(schema_dict['columns'])
        
        # Add metadata columns if requested
        if include_metadata and 'metadata_columns' in schema_dict:
            metadata_schema = SchemaLoader.build_spark_schema(schema_dict['metadata_columns'])
            # Combine both schemas
            all_fields = spark_schema.fields + metadata_schema.fields
            spark_schema = StructType(all_fields)
        
        return spark_schema
    
    @staticmethod
    def get_primary_keys(schema_dict: Dict[str, Any]) -> List[str]:
        """
        Extract primary key column names from schema.
        
        Args:
            schema_dict: Schema dictionary from load_schema()
            
        Returns:
            List of primary key column names
            
        Example:
            >>> schema = SchemaLoader.load_schema('config/bronze/orders_schema.json')
            >>> pks = SchemaLoader.get_primary_keys(schema)
            >>> print(pks)
            ['order_id']
        """
        return schema_dict.get('primary_keys', [])
    
    @staticmethod
    def get_partition_columns(schema_dict: Dict[str, Any]) -> List[str]:
        """
        Extract partition column names from schema.
        
        Args:
            schema_dict: Schema dictionary from load_schema()
            
        Returns:
            List of partition column names
            
        Example:
            >>> schema = SchemaLoader.load_schema('config/bronze/orders_schema.json')
            >>> partitions = SchemaLoader.get_partition_columns(schema)
            >>> print(partitions)
            ['ingestion_date']
        """
        return schema_dict.get('partition_by', [])
    
    @staticmethod
    def get_column_names(schema_dict: Dict[str, Any], 
                        include_metadata: bool = False) -> List[str]:
        """
        Extract all column names from schema.
        
        Args:
            schema_dict: Schema dictionary from load_schema()
            include_metadata: Whether to include metadata column names
            
        Returns:
            List of column names
            
        Example:
            >>> schema = SchemaLoader.load_schema('config/bronze/orders_schema.json')
            >>> cols = SchemaLoader.get_column_names(schema)
            >>> print(cols[:3])
            ['order_id', 'customer_id', 'order_date']
        """
        column_names = [col['name'] for col in schema_dict['columns']]
        
        if include_metadata and 'metadata_columns' in schema_dict:
            metadata_names = [col['name'] for col in schema_dict['metadata_columns']]
            column_names.extend(metadata_names)
        
        return column_names
    
    @staticmethod
    def get_nullable_columns(schema_dict: Dict[str, Any]) -> List[str]:
        """
        Extract names of nullable columns from schema.
        
        Args:
            schema_dict: Schema dictionary from load_schema()
            
        Returns:
            List of nullable column names
        """
        return [col['name'] for col in schema_dict['columns'] if col.get('nullable', True)]
    
    @staticmethod
    def get_required_columns(schema_dict: Dict[str, Any]) -> List[str]:
        """
        Extract names of required (non-nullable) columns from schema.
        
        Args:
            schema_dict: Schema dictionary from load_schema()
            
        Returns:
            List of required column names
        """
        return [col['name'] for col in schema_dict['columns'] if not col.get('nullable', True)]
    
    @staticmethod
    def get_table_info(schema_dict: Dict[str, Any]) -> Dict[str, str]:
        """
        Extract table metadata information from schema.
        
        Args:
            schema_dict: Schema dictionary from load_schema()
            
        Returns:
            Dictionary with table_name, source_system, description, etc.
            
        Example:
            >>> schema = SchemaLoader.load_schema('config/bronze/orders_schema.json')
            >>> info = SchemaLoader.get_table_info(schema)
            >>> print(info['table_name'])
            'bronze_orders'
        """
        return {
            'table_name': schema_dict.get('table_name'),
            'source_system': schema_dict.get('source_system'),
            'source_type': schema_dict.get('source_type'),
            'file_format': schema_dict.get('file_format'),
            'file_path': schema_dict.get('file_path'),
            'description': schema_dict.get('description'),
            'delimiter': schema_dict.get('delimiter'),
            'header': schema_dict.get('header')
        }
    
    @staticmethod
    def get_metadata_column_names(schema_dict: Dict[str, Any]) -> List[str]:
        """
        Extract metadata column names from schema.
        
        Args:
            schema_dict: Schema dictionary from load_schema()
            
        Returns:
            List of metadata column names
            
        Example:
            >>> schema = SchemaLoader.load_schema('config/bronze/orders_schema.json')
            >>> meta_cols = SchemaLoader.get_metadata_column_names(schema)
            >>> print(meta_cols)
            ['ingestion_timestamp', 'ingestion_date', 'source_file', 'record_source']
        """
        if 'metadata_columns' not in schema_dict:
            return []
        return [col['name'] for col in schema_dict['metadata_columns']]


# Convenience functions for direct use (functional interface)

def load_schema(schema_path: str) -> Dict[str, Any]:
    """
    Convenience function to load a schema file.
    
    Args:
        schema_path: Path to JSON schema file
        
    Returns:
        Schema dictionary
    """
    return SchemaLoader.load_schema(schema_path)


def build_spark_schema(columns: List[Dict[str, Any]]) -> StructType:
    """
    Convenience function to build a Spark schema from columns.
    
    Args:
        columns: List of column definitions
        
    Returns:
        PySpark StructType
    """
    return SchemaLoader.build_spark_schema(columns)


def get_primary_keys(schema_dict: Dict[str, Any]) -> List[str]:
    """
    Convenience function to get primary keys from schema.
    
    Args:
        schema_dict: Schema dictionary
        
    Returns:
        List of primary key column names
    """
    return SchemaLoader.get_primary_keys(schema_dict)


def get_partition_columns(schema_dict: Dict[str, Any]) -> List[str]:
    """
    Convenience function to get partition columns from schema.
    
    Args:
        schema_dict: Schema dictionary
        
    Returns:
        List of partition column names
    """
    return SchemaLoader.get_partition_columns(schema_dict)
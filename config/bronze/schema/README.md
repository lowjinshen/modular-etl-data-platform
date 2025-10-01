# Bronze Layer Schema Definitions

This directory contains JSON schema definitions for Bronze layer tables. These schemas drive the configuration-based ingestion framework.

## Schema Structure

Each schema file defines:

### **Core Metadata**
- `table_name` - Target Delta table name in Bronze layer
- `source_system` - Originating system identifier
- `source_type` - Currently supports "file"
- `file_format` - csv, json, or parquet
- `file_path` - Relative path to source file(s)
- `description` - Business description of the data

### **Table Configuration**
- `primary_keys` - List of columns that uniquely identify a record
- `partition_by` - Columns to partition Delta table by (for performance)
- `delimiter` - For CSV files (e.g., "|")
- `header` - Whether CSV has header row

### **Column Definitions**
Each column in the `columns` array specifies:
```json
{
  "name": "column_name",
  "datatype": "spark_datatype",
  "nullable": true/false,
  "description": "Business description"
}
```

### **Metadata Columns**
Standard audit columns added automatically to every Bronze table:
- `ingestion_timestamp` - When record was loaded
- `ingestion_date` - Date of load (used for partitioning)
- `source_file` - Original filename
- `record_source` - Source system identifier

## Supported Data Types

PySpark data types used in schemas:
- `string` - Text data
- `long` - 64-bit integer
- `integer` - 32-bit integer
- `decimal(p,s)` - Fixed precision decimal (p=precision, s=scale)
- `date` - Date without time
- `timestamp` - Date with time
- `boolean` - True/False

## Schema Files

| File | Source | Format | Records | Description |
|------|--------|--------|---------|-------------|
| `orders_schema.json` | orders.csv | CSV (pipe) | ~2,000 | Order transactions |
| `customers_schema.json` | customers_initial.json | JSON | 500 | Customer master data |
| `products_schema.json` | products.parquet | Parquet | 100 | Product catalog |
| `order_items_schema.json` | order_items.csv | CSV (pipe) | ~4,800 | Order line items |
| `inventory_schema.json` | inventory_*.json | JSON | 700 | Daily inventory snapshots |

## Usage in Bronze Ingestion

The Bronze ingestion framework reads these schemas to:

1. **Build Spark schema** for reading source files
   ```python
   schema = load_schema("config/bronze/orders_schema.json")
   spark_schema = build_spark_schema(schema['columns'])
   ```

2. **Validate data** against schema
   ```python
   df = spark.read.schema(spark_schema).format(file_format).load(file_path)
   ```

3. **Add metadata columns**
   ```python
   df = add_metadata_columns(df, schema['metadata_columns'])
   ```

4. **Create Delta table** with partitioning
   ```python
   df.write.format("delta") \
     .partitionBy(schema['partition_by']) \
     .save(delta_path)
   ```

## Schema Validation Rules

Bronze layer is **permissive but enforced**:
- ✅ Schema must match defined structure
- ✅ Non-nullable columns must have values
- ✅ Data types must be compatible
- ❌ NO business logic validation (happens in Silver)
- ❌ NO deduplication (happens in Silver)
- ❌ NO transformations (preserve raw data)

## Example: Reading Orders Schema

```python
import json

# Load schema
with open('config/bronze/orders_schema.json') as f:
    schema = json.load(f)

# Access schema properties
table_name = schema['table_name']  # 'bronze_orders'
primary_keys = schema['primary_keys']  # ['order_id']
columns = schema['columns']  # List of column definitions

# Get column names
column_names = [col['name'] for col in columns]

# Get nullable columns
nullable_cols = [col['name'] for col in columns if col['nullable']]
```

## Modifying Schemas

When source systems change:

1. **Adding a column** - Add to `columns` array
   ```json
   {
     "name": "new_column",
     "datatype": "string",
     "nullable": true,
     "description": "Description of new column"
   }
   ```

2. **Changing data type** - Update `datatype` property
   - Ensure compatibility with existing data
   - May require backfilling historical data

3. **Adding a source** - Create new schema file
   - Follow naming convention: `{source}_schema.json`
   - Include all required fields

## Best Practices

1. **Keep schemas in sync with source systems**
   - Document schema changes
   - Version control all changes

2. **Use descriptive names**
   - Clear column names
   - Detailed descriptions

3. **Be explicit about nullability**
   - Set `nullable: false` only when guaranteed
   - Bronze is permissive - strict validation happens in Silver

4. **Partition strategically**
   - Use `ingestion_date` for retention policies
   - Consider query patterns

5. **Document primary keys**
   - Essential for deduplication in Silver layer
   - Used for data quality checks

## Next Steps

After defining Bronze schemas:
1. Create Silver layer schemas (cleaned, deduplicated)
2. Create Gold layer schemas (dimensional model)
3. Build ingestion framework using these schemas

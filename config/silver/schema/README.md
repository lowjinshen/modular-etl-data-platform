# Silver Layer Schema Definitions

This directory contains JSON schema definitions for Silver layer tables. These schemas drive the cleansing and validation framework.

## Silver Layer Purpose

The Silver layer serves as the **single source of truth** with:
- ✅ Deduplicated records (keep latest version only)
- ✅ Data quality validation applied
- ✅ Referential integrity enforced
- ✅ Standardized formats and naming
- ✅ Business rules validation
- ✅ Upsert pattern (updates override previous versions)

**Data Flow:** Bronze → Silver (Cleanse & Validate) → Gold (Dimensional Model)

---

## Schema Structure

Each Silver schema file defines:

### **Core Metadata**
- `table_name` - Target Delta table name in Silver layer
- `source_table` - Source table from Bronze layer
- `source_system` - Originating system identifier
- `description` - Business description of the data
- `load_type` - "upsert" (default for Silver)

### **Table Configuration**
- `primary_keys` - Business keys for deduplication
- `partition_by` - Optional partitioning for large tables
- `incremental_column` - Column used for incremental processing (e.g., updated_timestamp from Bronze)

### **Column Definitions**
Each column in the `columns` array specifies:
```json
{
  "name": "column_name",
  "datatype": "spark_datatype",
  "nullable": true/false,
  "description": "Business description",
  "transformation": "Optional transformation logic"
}
```

### **Data Quality Rules**
Silver schemas include validation rules:
- `null_checks` - Columns that must not be null
- `uniqueness_checks` - Columns that must be unique
- `referential_integrity` - Foreign key validations
- `value_constraints` - Valid value ranges/lists
- `format_validations` - Format patterns (email, phone, etc.)

### **Metadata Columns**
Standard columns added to every Silver table:
- `updated_timestamp` - When record was last updated in Silver
- `record_source` - Source system identifier

---

## Supported Data Types

PySpark data types used in schemas:
- `string` - Text data
- `bigint` - 64-bit integer (replaces 'long' for consistency)
- `integer` - 32-bit integer
- `decimal(p,s)` - Fixed precision decimal (p=precision, s=scale)
- `date` - Date without time
- `timestamp` - Date with time
- `boolean` - True/False

---

## Schema Files

| File | Source | Primary Key | Records | Description |
|------|--------|-------------|---------|-------------|
| `customers_schema.json` | bronze_customers | customer_id | 500 | Cleansed customer master |
| `products_schema.json` | bronze_products | product_id | 100 | Cleansed product catalog |
| `orders_schema.json` | bronze_orders | order_id | ~2,000 | Deduplicated orders |
| `order_items_schema.json` | bronze_order_items | order_item_id | ~4,800 | Validated line items |
| `inventory_schema.json` | bronze_inventory | snapshot_date, product_id | 700 | Daily inventory snapshots |

---

## Transformation Rules

### **Data Cleansing Applied:**

1. **Text Standardization**
   - Trim leading/trailing whitespace
   - Title case for names (first_name, last_name, city)
   - Uppercase for codes (state, country)
   - Lowercase for emails

2. **Format Standardization**
   - Phone: XXX-XXX-XXXX format
   - Email: Validated format, lowercase
   - State: 2-character uppercase codes
   - Postal codes: Formatted consistently

3. **Data Type Conversions**
   - String dates → proper DATE types
   - Numeric strings → proper numeric types
   - Boolean standardization

4. **Null Handling**
   - Empty strings converted to NULL
   - Default values applied where appropriate
   - Required fields validated

### **Deduplication Strategy:**

Silver uses **upsert pattern** based on primary keys:
```
1. Read new records from Bronze (incremental by ingestion_date)
2. Deduplicate within batch (keep latest by ingestion_timestamp)
3. Merge with existing Silver table:
   - WHEN MATCHED → UPDATE (latest wins)
   - WHEN NOT MATCHED → INSERT
```

### **Referential Integrity:**

Foreign key validations enforced:
- `orders.customer_id` → `customers.customer_id`
- `order_items.order_id` → `orders.order_id`
- `order_items.product_id` → `products.product_id`
- `inventory.product_id` → `products.product_id`

Records failing validation can be:
- Logged to error table
- Quarantined for review
- Rejected with error message

---

## Data Quality Validation Rules

### **Level 1: Critical (Must Pass)**
- Primary key uniqueness
- Required field completeness
- Data type compatibility
- Referential integrity

### **Level 2: Warning (Flag for Review)**
- Suspicious value ranges
- Outlier detection
- Pattern mismatches
- Cross-field validation failures

### **Level 3: Informational**
- Missing optional fields
- Format recommendations
- Data enrichment opportunities

---

## Usage in Silver Transformation

The Silver transformation framework reads these schemas to:

1. **Load Bronze data incrementally**
   ```python
   bronze_df = spark.read.format("delta").load(source_table)
   # Filter by incremental_column if configured
   ```

2. **Apply transformations**
   ```python
   # Apply column-level transformations
   df = apply_transformations(bronze_df, schema['columns'])
   ```

3. **Validate data quality**
   ```python
   # Run validation rules
   validation_results = validate_data(df, schema['data_quality_rules'])
   ```

4. **Deduplicate records**
   ```python
   # Keep latest version by primary key
   df = deduplicate(df, schema['primary_keys'])
   ```

5. **Upsert to Silver table**
   ```python
   upsert(df, target_table, schema['primary_keys'])
   ```

---

## Example: Reading Silver Orders Schema

```python
from src.utils.schema_loader import SchemaLoader

# Load schema
schema = SchemaLoader.load_schema('config/silver/orders_schema.json')

# Access properties
table_name = schema['table_name']  # 'silver_orders'
source_table = schema['source_table']  # 'bronze_orders'
primary_keys = schema['primary_keys']  # ['order_id']

# Get data quality rules
dq_rules = schema['data_quality_rules']
null_checks = dq_rules['null_checks']  # Columns that can't be null
```

---

## Modifying Silver Schemas

When business rules change:

1. **Adding a transformation** - Update column's transformation property
   ```json
   {
     "name": "email",
     "datatype": "string",
     "nullable": false,
     "transformation": "lower(trim(email))"
   }
   ```

2. **Adding validation rule** - Update data_quality_rules section
   ```json
   "value_constraints": {
     "order_status": ["pending", "processing", "shipped", "delivered", "cancelled"]
   }
   ```

3. **Adding foreign key** - Update referential_integrity section
   ```json
   "referential_integrity": {
     "customer_id": {
       "reference_table": "silver_customers",
       "reference_column": "customer_id"
     }
   }
   ```

---

## Best Practices

1. **Keep transformations declarative**
   - Define transformations in schema when possible
   - Use SQL expressions for simplicity
   - Complex logic in separate transformation functions

2. **Validate early and often**
   - Check critical rules first (fail fast)
   - Log all validation failures
   - Maintain data quality metrics

3. **Maintain audit trail**
   - Track when records were updated
   - Preserve lineage to Bronze layer
   - Document transformation logic

4. **Incremental processing**
   - Process only new/changed records from Bronze
   - Use watermarking for idempotency
   - Partition for efficient reads

5. **Monitor data quality**
   - Track validation pass/fail rates
   - Alert on anomalies
   - Dashboard key metrics

---

## Silver Layer Guarantees

After Silver processing, data is guaranteed to be:

✅ **Unique** - No duplicate primary keys  
✅ **Valid** - Passes all data quality rules  
✅ **Complete** - Required fields populated  
✅ **Consistent** - Referential integrity maintained  
✅ **Standardized** - Formats normalized  
✅ **Clean** - No trailing spaces, case normalized  
✅ **Current** - Latest version only (no history)  

---

## Error Handling

Silver transformation handles errors gracefully:

**Validation Failures:**
- Log to `silver_dq_errors` table
- Include: table, column, rule, failed_value, timestamp
- Continue processing valid records

**Referential Integrity Failures:**
- Option 1: Reject record (strict mode)
- Option 2: Allow orphans, flag for review (permissive mode)
- Configurable per foreign key

**Transformation Failures:**
- Catch exceptions per record
- Log error details
- Use original value or NULL based on configuration

---

## Next Steps

After defining Silver schemas:
1. Build Silver transformation notebooks
2. Implement data quality validation framework
3. Create Gold dimensional schemas
4. Build end-to-end pipeline with Bronze → Silver → Gold
5. Add data quality monitoring dashboards

---

## Performance Considerations

### **Partition Strategy**
- Partition large fact tables by date (orders, inventory)
- Don't partition small dimension tables (customers, products)

### **Incremental Processing**
- Use `ingestion_date` from Bronze for filtering
- Process only recent partitions
- Maintain watermark table for progress tracking

### **Optimization**
- Z-ORDER by commonly filtered columns
- OPTIMIZE tables regularly
- Collect statistics for cost-based optimization

---

## Comparison: Bronze vs Silver

| Aspect | Bronze | Silver |
|--------|--------|--------|
| **Duplicates** | Allowed | Removed |
| **Nulls** | Allowed | Validated |
| **Formats** | Raw | Standardized |
| **History** | All versions | Latest only |
| **Validation** | Schema only | Business rules |
| **Purpose** | Audit trail | Analytics source |
| **Pattern** | Append-only | Upsert |

---

**Silver is production-ready data that downstream systems can trust!** 🥈

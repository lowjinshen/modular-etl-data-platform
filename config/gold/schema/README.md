# Gold Layer Schema Definitions

This directory contains JSON schema definitions for Gold layer tables. These schemas drive the dimensional modeling framework.

## Gold Layer Purpose

The Gold layer provides **analytics-ready dimensional models** with:
- ✅ Star schema design (facts + dimensions)
- ✅ SCD Type 2 for historical tracking
- ✅ Surrogate keys for relationships
- ✅ Business keys preserved
- ✅ Point-in-time dimension joins
- ✅ Pre-calculated measures
- ✅ Optimized for BI tools and reporting

**Data Flow:** Silver → Gold (Dimensional Modeling) → BI/Analytics

---

## Star Schema Architecture

```
                    ┌─────────────┐
                    │  dim_date   │
                    │────────────│
                    │ PK: date_key│
                    └─────┬───────┘
                          │
         ┌────────────────┼────────────────┐
         │                │                │
  ┌──────▼──────┐         │         ┌──────▼──────┐
  │dim_customer │         │         │ dim_product │
  │─────────────│         │         │─────────────│
  │PK: customer_│         │         │PK: product_ │
  │    key      │         │         │    key      │
  │BK: customer_│         │         │BK: product_ │
  │    id       │         │         │    id       │
  │(SCD Type 2) │         │         │(SCD Type 2) │
  └──────┬──────┘         │         └──────┬──────┘
         │                │                │
         │        ┌───────▼───────┐        │
         └────────► fact_orders   ◄────────┘
                  │───────────────│
                  │PK: order_key  │
                  │BK: order_id   │
                  └───────┬───────┘
                          │
                  ┌───────▼────────┐
                  │fact_order_items│
                  │────────────────│
                  │PK: order_item_ │
                  │    key         │
                  └────────────────┘
```

---

## Schema Structure

### **Dimension Schemas**

Each dimension schema defines:

**Core Metadata:**
- `table_name` - Target Delta table name
- `source_table` - Source from Silver layer
- `dimension_type` - "scd_type2" or "type1"
- `business_key` - Natural key from source system
- `surrogate_key` - Auto-generated primary key

**SCD Type 2 Configuration:**
- `type2_columns` - Columns to track history
- `type1_columns` - Columns to always update
- `scd_columns` - valid_from, valid_to, is_current, hash_diff

**Change Detection:**
- `hash_columns` - Columns included in MD5 hash for detecting changes

### **Fact Schemas**

Each fact schema defines:

**Core Metadata:**
- `table_name` - Target Delta table name
- `grain` - Business definition of one row
- `fact_type` - "transaction" or "snapshot"

**Relationships:**
- `dimension_keys` - Foreign keys to dimensions
- `measures` - Numeric facts for aggregation
- `calculated_measures` - Derived metrics

**Loading Strategy:**
- `load_type` - "incremental" or "full"
- `partition_by` - Partition strategy

---

## Supported Data Types

- `bigint` - Surrogate keys, business keys
- `integer` - Date keys (YYYYMMDD), measures
- `decimal(p,s)` - Monetary amounts, percentages
- `string` - Descriptive attributes
- `date` - Denormalized dates
- `timestamp` - SCD timestamps, audit columns
- `boolean` - Flags (is_current, is_weekend)

---

## Schema Files

### **Dimensions**

| File | Type | Business Key | Records | Description |
|------|------|--------------|---------|-------------|
| `dim_customer_schema.json` | SCD Type 2 | customer_id | ~550 | Customer history |
| `dim_product_schema.json` | SCD Type 2 | product_id | ~120 | Product price history |
| `dim_date_schema.json` | Type 1 | date_key | 1,095+ | Date dimension |

### **Facts**

| File | Grain | Records | Description |
|------|-------|---------|-------------|
| `fact_orders_schema.json` | One per order | ~2,000 | Order headers |
| `fact_order_items_schema.json` | One per line item | ~4,800 | Order details with profit |
| `fact_inventory_schema.json` | One per product per day | 700 | Daily inventory snapshots |

---

## SCD Type 2 Implementation

### **Change Detection Logic**

Gold dimensions use **hash-based change detection**:

```python
# Example: dim_customer hash calculation
hash_diff = MD5(
    address_line1 || '|' ||
    address_line2 || '|' ||
    city || '|' ||
    state || '|' ||
    postal_code || '|' ||
    customer_tier
)
```

**When hash changes:**
1. Close current record: `valid_to = current_timestamp, is_current = false`
2. Insert new record: `valid_from = current_timestamp, is_current = true`

**Type 1 columns always update** without creating new version:
- Customer: email, phone, first_name, last_name
- Product: product_name, category, brand, description

### **SCD Columns**

Every SCD Type 2 dimension includes:

| Column | Type | Description |
|--------|------|-------------|
| `valid_from` | TIMESTAMP | When this version became effective |
| `valid_to` | TIMESTAMP | When this version was superseded (9999-12-31 if current) |
| `is_current` | BOOLEAN | Flag for current version (true/false) |
| `hash_diff` | STRING | MD5 hash of Type 2 columns for change detection |

### **Example: Customer History**

```
customer_key | customer_id | city    | tier | valid_from | valid_to   | is_current
-------------|-------------|---------|------|------------|------------|------------
1            | 100         | Boston  | silver| 2024-01-01| 2024-06-15 | false
2            | 100         | Boston  | gold  | 2024-06-15| 2024-09-20 | false
3            | 100         | Chicago | gold  | 2024-09-20| 9999-12-31 | true
```

Query for current customers:
```sql
SELECT * FROM dim_customer WHERE is_current = true
```

Query for historical state:
```sql
SELECT * FROM dim_customer 
WHERE customer_id = 100 
  AND valid_from <= '2024-07-01' 
  AND valid_to > '2024-07-01'
```

---

## Point-in-Time Joins

Facts reference the **dimension version that was current at transaction time**:

```sql
-- Load fact_orders with correct customer version
INSERT INTO fact_orders
SELECT 
    o.order_id,
    c.customer_key,  -- Version of customer at order time
    o.order_date,
    o.order_total
FROM silver_orders o
JOIN dim_customer c 
    ON o.customer_id = c.customer_id
    AND o.order_date >= c.valid_from
    AND o.order_date < c.valid_to
```

This ensures reports show **what the customer attributes were when the order happened**.

---

## Surrogate Keys

### **Why Surrogate Keys?**

1. **Handle SCD Type 2** - Same business key has multiple versions
2. **Performance** - Integer joins faster than composite keys
3. **Flexibility** - Decouple from source system keys
4. **Consistency** - Uniform key structure across dimensions

### **Generation Strategy**

```python
# Auto-increment surrogate keys
customer_key = monotonically_increasing_id() or row_number()

# Ensures unique keys even across multiple loads
```

### **Key Relationships**

```
silver_customers.customer_id = 100  (Business Key)
    ↓
dim_customer (3 versions)
    customer_key = 1, customer_id = 100, valid_from = 2024-01-01
    customer_key = 2, customer_id = 100, valid_from = 2024-06-15
    customer_key = 3, customer_id = 100, valid_from = 2024-09-20
    ↓
fact_orders.customer_key = 2  (Points to specific version)
```

---

## Measures & Calculations

### **Additive Measures**
Can be summed across all dimensions:
- `order_total`, `tax_amount`, `shipping_fee`
- `quantity`, `line_total`
- `quantity_on_hand`, `stock_value`

### **Semi-Additive Measures**
Can be summed across some dimensions (not time):
- `quantity_on_hand` (snapshot - can't sum across dates)
- `stock_value` (use AVG or LAST value for time)

### **Calculated Measures**
Computed during Gold loading:
```sql
-- In fact_order_items
profit = (unit_price - product_cost) * quantity

-- In fact_inventory_snapshot
quantity_available = quantity_on_hand - quantity_reserved
stock_value = quantity_on_hand * unit_cost
```

---

## Date Dimension (dim_date)

### **Purpose**
- Enable time intelligence in BI tools
- Support fiscal calendar
- Provide date attributes (weekend, holiday, quarter)

### **Key Attributes**
- **date_key** (YYYYMMDD) - Primary key for joins
- **date** - Actual date value
- **year, quarter, month** - Hierarchies
- **day_of_week, day_name** - Weekday analysis
- **is_weekend, is_holiday** - Filtering
- **fiscal_year, fiscal_quarter** - Fiscal reporting

### **Generation**
```python
# Generate date dimension from min to max date + future
start_date = min(order_date) from silver_orders
end_date = current_date + 2 years
date_dim = generate_date_range(start_date, end_date)
```

---

## Usage in Gold Transformation

### **1. Load Dimensions with SCD Type 2**

```python
from src.utils.delta_operations import apply_scd_type2

# Load customer dimension
new_customers = spark.table("silver_customers")

apply_scd_type2(
    spark=spark,
    new_data=new_customers,
    primary_keys=["customer_id"],
    type2_columns=["address_line1", "city", "state", "customer_tier"],
    type1_columns=["email", "phone", "first_name", "last_name"],
    target_path="/mnt/gold/dim_customer",
    target_table="gold.dim_customer"
)
```

### **2. Load Facts with Point-in-Time Joins**

```python
# Load fact_orders
orders = spark.table("silver_orders")
customers = spark.table("gold.dim_customer")

fact_orders = orders.join(
    customers,
    (orders.customer_id == customers.customer_id) &
    (orders.order_date >= customers.valid_from) &
    (orders.order_date < customers.valid_to),
    "inner"
).select(
    orders.order_id,
    customers.customer_key,  # Surrogate key
    orders.order_date,
    orders.order_total,
    # ... other columns
)
```

---

## Data Quality in Gold

### **Dimension Quality Checks**
- All current records have `is_current = true`
- All closed records have `valid_to < 9999-12-31`
- No overlapping date ranges per business key
- All business keys have at least one current record

### **Fact Quality Checks**
- All dimension keys exist in dimension tables
- All date keys exist in date dimension
- Measures are within expected ranges
- No orphaned facts (missing dimension references)

---

## Performance Optimization

### **Partitioning Strategy**

**Dimensions:**
- Generally NOT partitioned (relatively small)
- Enable Z-ORDERING on business keys

**Facts:**
- Partition by date dimension key
- Enables partition pruning for time-based queries

```python
fact_orders
    .write
    .format("delta")
    .partitionBy("order_date_key")
    .save("/mnt/gold/fact_orders")
```

### **Optimization Commands**

```sql
-- Optimize dimension tables
OPTIMIZE gold.dim_customer ZORDER BY (customer_id);

-- Optimize fact tables
OPTIMIZE gold.fact_orders ZORDER BY (customer_key, order_date_key);

-- Collect statistics
ANALYZE TABLE gold.dim_customer COMPUTE STATISTICS;
```

---

## Query Patterns

### **Current State Analysis**
```sql
-- Current customer analysis
SELECT 
    c.customer_tier,
    COUNT(*) as customer_count,
    AVG(o.order_total) as avg_order_value
FROM fact_orders o
JOIN dim_customer c ON o.customer_key = c.customer_key
WHERE c.is_current = true
GROUP BY c.customer_tier;
```

### **Historical Analysis**
```sql
-- What was customer's tier when they placed orders?
SELECT 
    o.order_date,
    c.customer_tier as tier_at_order_time,
    SUM(o.order_total) as total_sales
FROM fact_orders o
JOIN dim_customer c ON o.customer_key = c.customer_key
GROUP BY o.order_date, c.customer_tier;
```

### **Time Series Analysis**
```sql
-- Daily sales trend with date attributes
SELECT 
    d.date,
    d.day_name,
    d.is_weekend,
    SUM(o.order_total) as daily_sales
FROM fact_orders o
JOIN dim_date d ON o.order_date_key = d.date_key
WHERE d.year = 2024 AND d.quarter = 3
GROUP BY d.date, d.day_name, d.is_weekend
ORDER BY d.date;
```

---

## Best Practices

### **Dimension Design**
1. **Choose Type 2 columns carefully** - Only track history for attributes that change and matter
2. **Keep Type 1 for corrections** - Email, phone should always be latest
3. **Generate hash on Type 2 columns only** - More efficient change detection
4. **Maintain referential integrity** - Every business key should have current version

### **Fact Design**
1. **Use surrogate keys** - Always join to dimensions via surrogate keys
2. **Denormalize selectively** - Business keys, key dates for convenience
3. **Pre-calculate when possible** - Profit, totals, percentages
4. **Partition appropriately** - By date for time-based analysis

### **Loading Process**
1. **Load dimensions first** - Facts depend on dimension keys
2. **Generate date dimension early** - Extend into future for planning
3. **Use point-in-time joins** - Ensure historical accuracy
4. **Validate relationships** - Check for orphaned facts

---

## Comparison: Silver vs Gold

| Aspect | Silver | Gold |
|--------|--------|------|
| **Keys** | Business keys | Surrogate + business keys |
| **History** | Latest only | Full history (SCD Type 2) |
| **Structure** | Normalized | Star schema |
| **Relationships** | Natural FKs | Surrogate key FKs |
| **Purpose** | Data quality | Analytics |
| **Users** | Data engineers | Business analysts, BI tools |
| **Queries** | ETL, validation | Reporting, dashboards |

---

## Next Steps

After defining Gold schemas:
1. Build dimension loading notebooks (SCD Type 2 logic)
2. Build fact loading notebooks (point-in-time joins)
3. Create data quality validation for Gold
4. Build sample BI queries/reports
5. Document analytics use cases

---

**Gold is where business value is realized through analytics!** 🥇

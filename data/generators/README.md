# E-commerce Sample Data Generator

This module generates realistic e-commerce sample data for demonstrating medallion architecture patterns, SCD Type 2 implementation, and data quality handling.

## Overview

The generator creates **realistic messy data** that mimics real-world scenarios data engineers face:

### Data Sources Generated

| File | Format | Purpose | Records |
|------|--------|---------|---------|
| `customers_initial.json` | JSON | Initial customer load | 500 |
| `customers_update.json` | JSON | Customer updates (SCD2) | ~50 |
| `products.parquet` | Parquet | Product catalog | 100 |
| `orders.csv` | CSV (pipe-delimited) | Order transactions | 2,000+ |
| `order_items.csv` | CSV (pipe-delimited) | Order line items | 4,000+ |
| `inventory_YYYY-MM-DD.json` | JSON | Daily snapshots | 700 (7 days × 100 products) |

## Data Quality Challenges Included

The generated data intentionally includes realistic data quality issues:

✅ **Duplicates** - ~2% duplicate orders (simulating upstream system issues)  
✅ **Missing Values** - Nulls in optional fields (notes, discount codes)  
✅ **Slowly Changing Dimensions** - ~10% customers have address changes  
✅ **Multiple Formats** - CSV, JSON, Parquet (typical enterprise reality)  
✅ **Different Delimiters** - Pipe-delimited CSV (non-standard)  
✅ **Referential Integrity** - Foreign key relationships to validate  

## Usage

### Installation

```bash
pip install -r requirements.txt
```

### Generate Data

```bash
cd data/generators
python generate_sample_data.py
```

Output will be created in `data/raw/` directory.

### Customize Generation

Edit configuration variables in `generate_sample_data.py`:

```python
NUM_CUSTOMERS = 500   # Number of customers
NUM_PRODUCTS = 100    # Number of products
NUM_ORDERS = 2000     # Number of orders
OUTPUT_DIR = "data/raw"
```

## Data Model

### Entity Relationships

```
CUSTOMERS (1) ----< (M) ORDERS (1) ----< (M) ORDER_ITEMS (M) >---- (1) PRODUCTS
                                                                            |
                                                                            |
                                                                    (1) ----< (M) INVENTORY
```

### Key Fields

**Customers**
- Primary Key: `customer_id`
- SCD Fields: `address_line1`, `city`, `state`, `customer_tier`
- Type 1 Fields: `email`, `phone`

**Orders**
- Primary Key: `order_id`
- Foreign Key: `customer_id`
- Status: `pending`, `processing`, `shipped`, `delivered`, `cancelled`

**Order Items**
- Primary Key: `order_item_id`
- Foreign Keys: `order_id`, `product_id`

**Products**
- Primary Key: `product_id`
- Includes pricing, category, brand information

**Inventory**
- Composite Key: `snapshot_date`, `product_id`
- Daily snapshot pattern

## Use Cases Demonstrated

This dataset enables demonstration of:

1. **Bronze Layer Ingestion**
   - Multi-format file reading
   - Schema enforcement
   - Data validation

2. **Silver Layer Transformation**
   - Deduplication
   - Data cleansing
   - Referential integrity checks

3. **Gold Layer Modeling**
   - SCD Type 2 (customer dimension)
   - Fact tables (orders, order items)
   - Periodic snapshots (inventory)

4. **Advanced Patterns**
   - Late-arriving dimensions
   - Slowly changing facts (order status)
   - Hash key generation
   - Audit column management

## Sample Data Preview

### Customers
```json
{
  "customer_id": 1,
  "first_name": "John",
  "last_name": "Smith",
  "email": "john.smith@example.com",
  "address_line1": "123 Main St",
  "city": "New York",
  "customer_tier": "gold"
}
```

### Orders (CSV)
```
order_id|customer_id|order_date|order_status|payment_method|order_total
1001|42|2024-09-15|delivered|credit_card|156.99
```

### Products (Parquet)
```
product_id | product_name | category | unit_price
1          | Widget Pro   | Electronics | 49.99
```

## Next Steps

After generating data:
1. Run Bronze layer ingestion notebooks
2. Apply data quality rules in Silver layer
3. Build dimensional model in Gold layer
4. Demonstrate SCD Type 2 with customer updates

## Regenerating Data

To create fresh data with different characteristics:
```bash
python generate_sample_data.py
```

The script uses a fixed random seed (42) for reproducibility. Change the seed for different data patterns.

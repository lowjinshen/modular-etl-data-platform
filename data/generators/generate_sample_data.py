"""
E-commerce Sample Data Generator

Generates realistic sample data for demonstrating medallion architecture:
- Orders (CSV) - transactional data with some quality issues
- Customers (JSON) - slowly changing dimension data
- Products (Parquet) - catalog data
- Order Items (CSV) - line item details
- Daily Inventory Snapshots (JSON) - periodic snapshots

Features realistic data issues:
- Duplicates
- Missing values
- Late-arriving data
- Changing customer addresses (SCD Type 2 scenarios)
- Referential integrity challenges
"""

import random
import pandas as pd
import json
from datetime import datetime, timedelta
from faker import Faker
import os

# Set random seed for reproducibility
random.seed(42)
Faker.seed(42)
fake = Faker()

# Configuration
NUM_CUSTOMERS = 500
NUM_PRODUCTS = 100
NUM_ORDERS = 2000
OUTPUT_DIR = "data/raw"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("🎲 Starting E-commerce Data Generation...")
print(f"📊 Generating {NUM_CUSTOMERS} customers, {NUM_PRODUCTS} products, {NUM_ORDERS} orders")


# ============================================================================
# CUSTOMERS - JSON format, with SCD Type 2 scenarios
# ============================================================================
def generate_customers():
    """Generate customer master data with address changes over time"""
    print("\n👥 Generating customers...")
    
    customers = []
    customer_changes = []  # Track customers who will have address changes
    
    for customer_id in range(1, NUM_CUSTOMERS + 1):
        # Initial customer record
        customer = {
            'customer_id': customer_id,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address_line1': fake.street_address(),
            'address_line2': fake.secondary_address() if random.random() > 0.7 else None,
            'city': fake.city(),
            'state': fake.state_abbr(),
            'postal_code': fake.zipcode(),
            'country': 'USA',
            'customer_status': random.choice(['active', 'active', 'active', 'inactive']),
            'registration_date': (datetime.now() - timedelta(days=random.randint(30, 730))).strftime('%Y-%m-%d'),
            'customer_tier': random.choice(['bronze', 'silver', 'gold', 'platinum']),
            'marketing_opt_in': random.choice([True, False])
        }
        
        customers.append(customer)
        
        # 10% of customers will have address changes (for SCD Type 2 demo)
        if random.random() < 0.1:
            customer_changes.append(customer_id)
    
    # Create initial customer file
    with open(f"{OUTPUT_DIR}/customers_initial.json", 'w') as f:
        json.dump(customers, f, indent=2)
    
    print(f"✅ Generated {len(customers)} customers (initial load)")
    
    # Generate updated customer records (simulate address changes)
    updated_customers = []
    for customer_id in customer_changes:
        original = customers[customer_id - 1].copy()
        original['address_line1'] = fake.street_address()
        original['city'] = fake.city()
        original['state'] = fake.state_abbr()
        original['postal_code'] = fake.zipcode()
        # Simulate tier changes too
        original['customer_tier'] = random.choice(['silver', 'gold', 'platinum'])
        updated_customers.append(original)
    
    if updated_customers:
        with open(f"{OUTPUT_DIR}/customers_update.json", 'w') as f:
            json.dump(updated_customers, f, indent=2)
        print(f"✅ Generated {len(updated_customers)} customer updates (for SCD Type 2 demo)")
    
    return customers


# ============================================================================
# PRODUCTS - Parquet format
# ============================================================================
def generate_products():
    """Generate product catalog"""
    print("\n📦 Generating products...")
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food & Beverage']
    
    products = []
    for product_id in range(1, NUM_PRODUCTS + 1):
        category = random.choice(categories)
        product = {
            'product_id': product_id,
            'product_name': fake.catch_phrase(),
            'category': category,
            'subcategory': f"{category} - {fake.word().title()}",
            'brand': fake.company(),
            'unit_price': round(random.uniform(5.99, 999.99), 2),
            'cost': round(random.uniform(2.99, 500.00), 2),
            'description': fake.text(max_nb_chars=200),
            'weight_kg': round(random.uniform(0.1, 50.0), 2),
            'is_active': random.choice([True, True, True, False]),  # 75% active
            'created_date': (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d')
        }
        products.append(product)
    
    df_products = pd.DataFrame(products)
    df_products.to_parquet(f"{OUTPUT_DIR}/products.parquet", index=False)
    
    print(f"✅ Generated {len(products)} products")
    return products


# ============================================================================
# ORDERS - CSV format with data quality issues
# ============================================================================
def generate_orders(customers):
    """Generate orders with intentional data quality issues"""
    print("\n🛒 Generating orders...")
    
    orders = []
    order_statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
    payment_methods = ['credit_card', 'debit_card', 'paypal', 'bank_transfer']
    
    for order_id in range(1, NUM_ORDERS + 1):
        customer = random.choice(customers)
        order_date = datetime.now() - timedelta(days=random.randint(0, 180))
        
        order = {
            'order_id': order_id,
            'customer_id': customer['customer_id'],
            'order_date': order_date.strftime('%Y-%m-%d'),
            'order_time': order_date.strftime('%H:%M:%S'),
            'order_status': random.choice(order_statuses),
            'payment_method': random.choice(payment_methods),
            'shipping_address': customer['address_line1'],
            'shipping_city': customer['city'],
            'shipping_state': customer['state'],
            'shipping_postal_code': customer['postal_code'],
            'order_total': None,  # Will calculate from items
            'tax_amount': None,
            'shipping_fee': round(random.uniform(0, 15.99), 2) if random.random() > 0.3 else 0,
            'discount_code': fake.word().upper() if random.random() > 0.8 else None,
            'discount_amount': round(random.uniform(5, 50), 2) if random.random() > 0.8 else 0,
            'notes': fake.sentence() if random.random() > 0.9 else None
        }
        
        orders.append(order)
    
    # Introduce data quality issues
    # 1. Add duplicates (2%)
    num_duplicates = int(len(orders) * 0.02)
    duplicates = random.sample(orders, num_duplicates)
    orders.extend(duplicates)
    
    # 2. Add some nulls in non-critical fields (5%)
    for order in random.sample(orders, int(len(orders) * 0.05)):
        order['notes'] = None
        order['discount_code'] = None
    
    # 3. Shuffle to make duplicates non-obvious
    random.shuffle(orders)
    
    # Save to CSV
    df_orders = pd.DataFrame(orders)
    df_orders.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False, sep='|')
    
    print(f"✅ Generated {len(orders)} orders (including {num_duplicates} duplicates)")
    return orders


# ============================================================================
# ORDER ITEMS - CSV format
# ============================================================================
def generate_order_items(orders, products):
    """Generate order line items"""
    print("\n📝 Generating order items...")
    
    order_items = []
    item_id = 1
    
    for order in orders[:NUM_ORDERS]:  # Exclude duplicates for item generation
        # Each order has 1-5 items
        num_items = random.randint(1, 5)
        selected_products = random.sample(products, min(num_items, len(products)))
        
        for product in selected_products:
            quantity = random.randint(1, 3)
            unit_price = product['unit_price']
            line_total = round(quantity * unit_price, 2)
            
            order_item = {
                'order_item_id': item_id,
                'order_id': order['order_id'],
                'product_id': product['product_id'],
                'quantity': quantity,
                'unit_price': unit_price,
                'line_total': line_total,
                'discount_applied': round(line_total * 0.1, 2) if random.random() > 0.9 else 0
            }
            
            order_items.append(order_item)
            item_id += 1
    
    # Update order totals based on items (in real scenario, this would be calculated)
    order_totals = {}
    for item in order_items:
        order_id = item['order_id']
        if order_id not in order_totals:
            order_totals[order_id] = 0
        order_totals[order_id] += item['line_total'] - item['discount_applied']
    
    for order in orders:
        if order['order_id'] in order_totals:
            subtotal = order_totals[order['order_id']]
            order['order_total'] = round(subtotal + order['shipping_fee'] - order['discount_amount'], 2)
            order['tax_amount'] = round(subtotal * 0.08, 2)  # 8% tax
    
    # Save to CSV
    df_items = pd.DataFrame(order_items)
    df_items.to_csv(f"{OUTPUT_DIR}/order_items.csv", index=False, sep='|')
    
    # Re-save orders with updated totals
    df_orders = pd.DataFrame(orders)
    df_orders.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False, sep='|')
    
    print(f"✅ Generated {len(order_items)} order items")
    return order_items


# ============================================================================
# INVENTORY SNAPSHOTS - JSON format
# ============================================================================
def generate_inventory_snapshots(products):
    """Generate daily inventory snapshots"""
    print("\n📊 Generating inventory snapshots...")
    
    # Generate snapshots for last 7 days
    snapshots = []
    
    for days_ago in range(7, 0, -1):
        snapshot_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        for product in products:
            snapshot = {
                'snapshot_date': snapshot_date,
                'product_id': product['product_id'],
                'quantity_on_hand': random.randint(0, 1000),
                'quantity_reserved': random.randint(0, 100),
                'reorder_point': random.randint(10, 100),
                'warehouse_location': random.choice(['WH-A', 'WH-B', 'WH-C', 'WH-D']),
                'last_restock_date': (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d')
            }
            snapshots.append(snapshot)
    
    # Save by date (simulating daily files)
    for days_ago in range(7, 0, -1):
        snapshot_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        daily_snapshots = [s for s in snapshots if s['snapshot_date'] == snapshot_date]
        
        filename = f"{OUTPUT_DIR}/inventory_{snapshot_date}.json"
        with open(filename, 'w') as f:
            json.dump(daily_snapshots, f, indent=2)
    
    print(f"✅ Generated {len(snapshots)} inventory records across 7 daily files")


# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    """Generate all sample data"""
    
    # Generate data
    customers = generate_customers()
    products = generate_products()
    orders = generate_orders(customers)
    order_items = generate_order_items(orders, products)
    generate_inventory_snapshots(products)
    
    print("\n" + "="*60)
    print("✨ Data Generation Complete!")
    print("="*60)
    print(f"\n📁 Files created in '{OUTPUT_DIR}/':")
    print("  - customers_initial.json (initial customer load)")
    print("  - customers_update.json (customer updates for SCD2)")
    print("  - products.parquet (product catalog)")
    print("  - orders.csv (orders with duplicates)")
    print("  - order_items.csv (order line items)")
    print("  - inventory_YYYY-MM-DD.json (7 daily snapshots)")
    
    print("\n📊 Data Characteristics:")
    print(f"  - {NUM_CUSTOMERS} customers (with ~10% having address changes)")
    print(f"  - {NUM_PRODUCTS} products")
    print(f"  - {NUM_ORDERS} orders (plus ~2% duplicates)")
    print(f"  - {len(order_items)} order items")
    print(f"  - {NUM_PRODUCTS * 7} inventory records (7 days)")
    
    print("\n🎯 Data Quality Issues Included:")
    print("  ✓ Duplicate orders (~2%)")
    print("  ✓ Missing values in optional fields")
    print("  ✓ Customer address changes (SCD Type 2 scenarios)")
    print("  ✓ Multiple file formats (CSV, JSON, Parquet)")
    print("  ✓ Different delimiters (pipe-delimited CSV)")
    
    print("\n🚀 Ready for Bronze layer ingestion!")


if __name__ == "__main__":
    main()

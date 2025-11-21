import boto3
from datetime import datetime

def setup_realtime_dynamodb():
    """Set up DynamoDB for real-time inventory tracking"""
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Create real-time inventory table
    try:
        table = dynamodb.create_table(
            TableName='RealTimeInventory',
            KeySchema=[
                {'AttributeName': 'product_id', 'KeyType': 'HASH'},
                {'AttributeName': 'store_location', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'product_id', 'AttributeType': 'S'},
                {'AttributeName': 'store_location', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        print("✅ Creating RealTimeInventory table...")
        table.meta.client.get_waiter('table_exists').wait(TableName='RealTimeInventory')
        print("✅ Real-time table created successfully!")
        
    except Exception as e:
        print(f"Table may already exist: {e}")
    
    # Populate with current inventory from your CSV data
    table = dynamodb.Table('RealTimeInventory')
    
    # Using data from your Athena results
    realtime_inventory = [
        # Current stock levels from your system
        {'product_id': 'P001', 'store_location': 'NYC-Store-1', 'current_stock': 47, 'reorder_point': 20, 'max_capacity': 100, 'supplier_id': 'SUP001'},
        {'product_id': 'P001', 'store_location': 'LA-Store-1', 'current_stock': 38, 'reorder_point': 20, 'max_capacity': 100, 'supplier_id': 'SUP001'},
        {'product_id': 'P001', 'store_location': 'Chicago-Store-1', 'current_stock': 41, 'reorder_point': 20, 'max_capacity': 100, 'supplier_id': 'SUP001'},
        {'product_id': 'P001', 'store_location': 'Miami-Store-1', 'current_stock': 25, 'reorder_point': 20, 'max_capacity': 100, 'supplier_id': 'SUP001'},  # LOW STOCK
        
        {'product_id': 'P002', 'store_location': 'NYC-Store-1', 'current_stock': 40, 'reorder_point': 15, 'max_capacity': 80, 'supplier_id': 'SUP002'},
        {'product_id': 'P002', 'store_location': 'LA-Store-1', 'current_stock': 47, 'reorder_point': 15, 'max_capacity': 80, 'supplier_id': 'SUP002'},
        {'product_id': 'P002', 'store_location': 'Chicago-Store-1', 'current_stock': 23, 'reorder_point': 15, 'max_capacity': 80, 'supplier_id': 'SUP002'},
        {'product_id': 'P002', 'store_location': 'Miami-Store-1', 'current_stock': 18, 'reorder_point': 15, 'max_capacity': 80, 'supplier_id': 'SUP002'},
        
        {'product_id': 'P003', 'store_location': 'NYC-Store-1', 'current_stock': 67, 'reorder_point': 25, 'max_capacity': 120, 'supplier_id': 'SUP001'},
        {'product_id': 'P003', 'store_location': 'LA-Store-1', 'current_stock': 66, 'reorder_point': 25, 'max_capacity': 120, 'supplier_id': 'SUP001'},
        
        # Add critical low stock items for real-time alert testing
        {'product_id': 'P004', 'store_location': 'Miami-Store-1', 'current_stock': 8, 'reorder_point': 10, 'max_capacity': 50, 'supplier_id': 'SUP003'},  # CRITICAL
        {'product_id': 'P005', 'store_location': 'Miami-Store-1', 'current_stock': 3, 'reorder_point': 5, 'max_capacity': 30, 'supplier_id': 'SUP003'},   # CRITICAL
    ]
    
    print("🔄 Populating real-time inventory with current stock levels...")
    for item in realtime_inventory:
        table.put_item(Item=item)
        
        # Show stock status
        if item['current_stock'] <= 5:
            status = "🚨 CRITICAL"
        elif item['current_stock'] <= item['reorder_point']:
            status = "⚠️ LOW"
        else:
            status = "✅ OK"
            
        print(f"✅ {item['product_id']} at {item['store_location']}: {item['current_stock']} units ({status})")
    
    print("\n🚀 Real-time DynamoDB inventory system is LIVE!")
    return table

# Execute real-time setup
if __name__ == "__main__":
    setup_realtime_dynamodb()


import boto3
import time

def create_realtime_kinesis_streams():
    """Create Kinesis streams for real-time retail transaction processing"""
    
    kinesis = boto3.client('kinesis', region_name='us-east-1')
    
    streams_config = [
        {'StreamName': 'retail-sales-stream', 'ShardCount': 2},      # Main sales transactions
        {'StreamName': 'retail-inventory-alerts', 'ShardCount': 1},  # Low stock alerts
        {'StreamName': 'retail-customer-events', 'ShardCount': 1}    # Customer activities
    ]
    
    print("🚀 Creating real-time Kinesis streams for retail analytics...")
    print("=" * 60)
    
    created_streams = []
    
    for stream in streams_config:
        try:
            response = kinesis.create_stream(**stream)
            print(f"✅ Created stream: {stream['StreamName']} with {stream['ShardCount']} shards")
            created_streams.append(stream['StreamName'])
            
        except kinesis.exceptions.ResourceInUseException:
            print(f"⚠️  Stream {stream['StreamName']} already exists - continuing...")
            created_streams.append(stream['StreamName'])
            
        except Exception as e:
            print(f"❌ Error creating {stream['StreamName']}: {e}")
    
    print(f"\n⏳ Waiting for streams to become active...")
    time.sleep(20)  # Wait for stream activation
    
    # Check stream status
    print("\n📊 Real-Time Stream Status:")
    all_active = True
    
    for stream_name in created_streams:
        try:
            response = kinesis.describe_stream(StreamName=stream_name)
            status = response['StreamDescription']['StreamStatus']
            shards = len(response['StreamDescription']['Shards'])
            
            if status == "ACTIVE":
                status_icon = "✅"
                print(f"   {status_icon} {stream_name}: {status} ({shards} shards) - READY FOR REAL-TIME DATA")
            else:
                status_icon = "⏳"
                all_active = False
                print(f"   {status_icon} {stream_name}: {status} ({shards} shards) - Still activating...")
            
        except Exception as e:
            print(f"   ❌ Error checking {stream_name}: {e}")
            all_active = False
    
    if all_active:
        print(f"\n🎉 All real-time data streams are ACTIVE and ready!")
        print("📡 Your system can now receive live transactions from POS systems")
    else:
        print(f"\n⏳ Some streams still activating - wait 30 more seconds and check AWS Console")
    
    print(f"\n🔗 Stream Endpoints Created:")
    for stream_name in created_streams:
        print(f"   • {stream_name}: Ready for real-time data ingestion")
    
    return created_streams

# Execute Kinesis setup
if __name__ == "__main__":
    create_realtime_kinesis_streams()


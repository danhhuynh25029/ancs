from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
  bootstrap_servers=["localhost:19092"],
  group_id="demo-group",
  auto_offset_reset="earliest",
  enable_auto_commit=False,
#   consumer_timeout_ms=1000
)

consumer.subscribe("dbz.shops.orders")


try:
    for message in consumer:
        topic_info = f"topic: {message.partition}|{message.offset}"
        message_info = f"key: {message.value}"
        try:
            final_dict = json.loads(message.value) 
            print(final_dict['payload']['after'])
        
        except Exception as e:
            print("Invalid JSON syntax:", e)
            continue
except Exception as e:
    print(f"Error occurred while consuming messages: {e}")
finally:
    consumer.close()



from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'my-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False
)

print("Waiting for messages...")

while True:
    msg_pack = consumer.poll(timeout_ms=1000)
    if msg_pack:
        for tp, messages in msg_pack.items():
            for msg in messages:
                print(f"Topic: {tp.topic} | Partition: {tp.partition} | Offset: {msg.offset} | Key: {msg.key} | Value: {msg.value.decode('utf-8')}")
    else:
        print("No new messages")

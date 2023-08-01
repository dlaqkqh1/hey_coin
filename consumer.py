from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata

BROKERS = ('localhost:9092', 'localhost:9093', 'localhost:9094')
consumer_group_id = "hey_coin"

consumer = KafkaConsumer(
    bootstrap_servers=BROKERS,
    group_id=consumer_group_id,
    key_deserializer=lambda x: x.decode('utf-8'),
    value_deserializer=lambda x: x.decode('utf-8'),
    auto_offset_reset='latest',
    consumer_timeout_ms=30000,
    enable_auto_commit=False)

TICKER_LIST = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-DOGE', 'KRW-ETC']
consumer.subscribe(TICKER_LIST)

for record in consumer:
    print(f"""{record.topic}: 데이터 {record.value} 를 파티션 {record.partition} 의 {record.offset} 번 오프셋에서 읽어옴""")
    topic_partition = TopicPartition(record.topic, record.partition)
    offset = OffsetAndMetadata(record.offset + 1, record.timestamp)
    consumer.commit({
        topic_partition: offset
    })

consumer.close()

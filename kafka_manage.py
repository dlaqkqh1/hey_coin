from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer
from kafka import KafkaAdminClient


def create_topic(bootstrap_servers: tuple, name: str, partition: int = 2, replica: int = 3):
    client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    try:
        topic = NewTopic(
            name=name,
            num_partitions=partition,
            replication_factor=replica)
        client.create_topics([topic])
    except TopicAlreadyExistsError as e:
        print(e)
        pass
    finally:
        client.close()


def get_producer(bootstrap_servers: tuple):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id="hey_coin",
        acks=0
    )

    return producer

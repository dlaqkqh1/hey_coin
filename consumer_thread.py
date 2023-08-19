from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from threading import Thread
import queue


class NewThread(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}):
        Thread.__init__(self, group, target, name, args, kwargs)

    def run(self):
        if self._target != None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self, *args):
        Thread.join(self, *args)
        return self._return


class ConsumerTread:
    def __init__(self):
        BROKERS = ('localhost:9092', 'localhost:9093', 'localhost:9094')
        consumer_group_id = "hey_coin"

        consumer_args = {'bootstrap_servers': BROKERS, 'group_id': consumer_group_id,
                         'key_deserializer': lambda x: x.decode('utf-8'),
                         'value_deserializer': lambda x: x.decode('utf-8'),
                         'auto_offset_reset': 'latest',
                         'consumer_timeout_ms': 30000,
                         'enable_auto_commit': False
                         }

        self.consumer1 = KafkaConsumer(**consumer_args)
        self.consumer2 = KafkaConsumer(**consumer_args)

        TICKER_LIST = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP', 'KRW-DOGE', 'KRW-ETC']
        self.consumer1.subscribe(TICKER_LIST)
        self.consumer2.subscribe(TICKER_LIST)

        self.data_queue = queue.Queue()
        self.thread1 = NewThread(target=self.consume_messages, args=(self.consumer1, "Consumer 1"))
        self.thread2 = NewThread(target=self.consume_messages, args=(self.consumer2, "Consumer 2"))

    def start_consuming(self):
        self.thread1.start()
        self.thread2.start()

        return self.thread1, self.thread2, self.consumer1, self.consumer2

        # try:
        #     self.thread1.join()
        #     self.thread2.join()
        #
        # except KeyboardInterrupt:
        #     pass
        #
        # finally:
        #     self.consumer1.close()
        #     self.consumer2.close()

    @staticmethod
    def consume_messages(consumer, name):
        for record in consumer:
            result = f"{name}/{record.topic}: 데이터 {record.value} 를 파티션 {record.partition} 의 {record.offset} 번 오프셋에서 읽어옴"
            topic_partition = TopicPartition(record.topic, record.partition)
            offset = OffsetAndMetadata(record.offset + 1, record.timestamp)
            consumer.commit({
                topic_partition: offset
            })
            return result



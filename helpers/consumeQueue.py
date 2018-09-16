from confluent_kafka import Consumer, KafkaError

class ConsumeQueue:
    def readQueue(topicName):
        settings = {
            'bootstrap.servers': 'localhost',
            'group.id': 'mygroup',
            'client.id': 'client-1',
            'enable.auto.commit': True,
            'session.timeout.ms': 6000,
            'default.topic.config': {'auto.offset.reset': 'smallest'}
        }
        consumer = Consumer(settings)
        consumer.subscribe([topicName])
        try:
            while True:
                msg = consumer.poll(0.1)
                if msg is None:
                    continue
                elif not msg.error():
                    print('Received message: {0}'.format(msg.value()))
                    return msg.value()
                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print('Error occured: {0}'.format(msg.error().str()))

        except KeyboardInterrupt:
            pass

        finally:
            consumer.close()
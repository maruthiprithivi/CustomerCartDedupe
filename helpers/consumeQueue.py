from confluent_kafka import Consumer, KafkaError
from helpers.YamlReader import YamlReader

class ConsumeQueue:
    def readQueue(path):
        config = YamlReader.ymlConfig(path)
        host = config['kafka']['host']
        groupId = config['kafka']['groupid']
        clientId = config['kafka']['clientid']
        topicName = config['kafka']['topic']
        settings = {
            'bootstrap.servers': host,
            'group.id': groupId,
            'client.id': clientId,
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
                    message = msg.value().decode("utf-8")
                    print('Received message: {0}'.format(message))
                    return message
                elif msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print('Error occured: {0}'.format(msg.error().str()))

        except KeyboardInterrupt:
            pass

        finally:
            consumer.close()
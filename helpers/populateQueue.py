from confluent_kafka import Producer
from helpers.YamlReader import YamlReader

class PopulateQueue:
    def writeMessage(message, path):
        config = YamlReader.ymlConfig(path)
        topicName = config['kafka']['topic']
        def acked(err, msg):
            if err is not None:
                print("Failed to deliver message: {0}: {1}"
                      .format(msg.value(), err.str()))
            else:
                print("Message produced: {0}".format(msg.value()))

        producer = Producer({'bootstrap.servers': 'localhost:9021'})

        try:
            producer.produce(topicName, message, callback=acked)
            producer.poll(0.5)

        except KeyboardInterrupt:
            pass
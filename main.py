import sys
from helpers.consumeQueue import ConsumeQueue as cq
from helpers.populateQueue import PopulateQueue as pq

if len(sys.argv) == 3:
    workerType = sys.argv[1]
    topicName = sys.argv[2]
    if workerType == "consume":
        msg = cq.readQueue(topicName)
        print("Test run is successful {0}".format(msg))
elif len(sys.argv) == 4:
    workerType = sys.argv[1]
    topicName = sys.argv[2]
    msg = sys.argv[3]
    if workerType == "produce":
        pq.writeMessage(topicName,msg)

else:
    print("[Usage]: python3 <App Main> consume/produce <Topic Name>")
    print("[Example]: python3 main.py consume someTopicName")
    print("[Usage]: python3 <App Main> produce <Topic Name> <json message>")
    print("[Example]: python3 main.py produce someTopicName '{'someJsonMessageGoesHere': 1}'")

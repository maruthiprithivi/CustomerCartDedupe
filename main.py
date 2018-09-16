import sys
import json
from helpers.consumeQueue import ConsumeQueue as cq
from helpers.populateQueue import PopulateQueue as pq
from helpers.MongoConnect import MongoConnect as mc
from helpers.MongoRW import MongoRW as mw
from helpers.YamlReader import YamlReader


if len(sys.argv) == 2:
    path = sys.argv[1]
    connect = mc.mongoEstablish(path)
    allRecs = mw.fetchAll(connect)
    print(allRecs)

elif len(sys.argv) == 3:
    workerType = sys.argv[1]
    path = sys.argv[2]
    config = YamlReader.ymlConfig(path)
    mapper1 = config['logicone']['mapper1']
    mapper2 = config['logicone']['mapper2']
    mapper3 = config['logicone']['mapper3']
    mapper4 = config['logicone']['mapper4']
    if workerType == "consume":
        msg = cq.readQueue(path)
        msgJson = json.loads(msg)
        connect = mc.mongoEstablish(path)
        response = mw.upsertCart(connect, msgJson, mapper1, mapper2, mapper3, mapper4)
        # print("This is the product that came in the message: {0}".format(msgJson[product]))

elif len(sys.argv) == 4:
    workerType = sys.argv[1]
    path = sys.argv[2]
    msg = sys.argv[3]
    if workerType == "produce":
        pq.writeMessage(msg, path)

else:
    print("[Usage]: python3 <App Main> consume <Config File YAML>")
    print("[Example]: python3 main.py consume /some/directory/path/config.yml")
    print("[Usage]: python3 <App Main> produce <Config File YAML> <json message>")
    print("[Example]: python3 main.py produce /some/directory/path/config.yml '{'someJsonMessageGoesHere': 1}' ")

from pymongo import MongoClient
from helpers.YamlReader import YamlReader


class MongoConnect:
    userName = str
    password = str
    host = str

    def mongoEstablish(path):
        config = YamlReader.ymlConfig(path)
        userName = str
        password = str
        host = str
        port = str
        database = str
        collection = str
        try:
            userName = config['mongo']['username']
            password = config['mongo']['password']
            host = config['mongo']['host']
            port = config['mongo']['port']
            database = config['mongo']['database']
            collection = config['mongo']['collection']
        except:
            print("We seem to be missing part of the configuration, the system might go to the default")
        if userName and password is not None:
            client = MongoClient('mongodb://' + userName + ':' + password + '@' + host + ':' + str(port) + '/')
        elif host and port is not None:
            client = MongoClient('mongodb://' + host + ':' + port + '/')
        else:
            client = MongoClient('mongodb://127.0.0.1:27017/', w=1)
        db = client[database]
        Collection = db[collection]
        return Collection  # .with_options(write_concern=WriteConcern(w=1))

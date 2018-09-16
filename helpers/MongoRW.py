from pymongo import errors
from pymongo import DESCENDING

class MongoRW:
    def upsertCart(collection, data, mapper1, mapper2, mapper3, mapper4):

        # This index is aimed at addressing the duplicate item in the cart thing
        try:
            collection.create_index([(mapper1, DESCENDING), (mapper2, DESCENDING), (mapper3, DESCENDING), (mapper4, DESCENDING)], name='cartAndProduct_unique', unique=True)
        except:
            print("[Info]: Unique index already exists")
        count = 0
        try:
            action = collection.insert_one(data)
            print(dir(action))
            print(action.inserted_id)
            # Write concerns future extension
            if action.inserted_id is not None:
                count += 1
        except errors.DuplicateKeyError:
            print("#" * 20)
            print("Duplicate Item in the cart!")
            print("#" * 20)

    def fetchAll(collection):
        records = []
        allDocs = collection.find({})
        for record in allDocs:
            print("Record {0}".format(record))
            records.append(record)
            return records

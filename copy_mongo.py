from base64 import standard_b64decode
from pprint import pprint
from optparse import OptionParser
from datetime import date, timedelta
import sys

from pymongo import MongoClient, collection, cursor
from pymongo.errors import BulkWriteError

from bson import objectid
from bson.son import SON





SOURCE_URI = "mongodb://admin:JMBTXCSOUOERIQDG@sl-us-dal-9-portal.0.dblayer.com:17820,sl-us-dal-9-portal.3.dblayer.com:17820/admin?ssl=true"
DESTINATION_URI = "mongodb://0.0.0.0:27017"


def parse_args():
    parser = OptionParser(usage="Usage: %prog [options]",
                          description="Copy data from Trevor's mongodb to your locally running mongodb.")
    parser.add_option("-s", "--start-date",
                      dest="start_date", default="20170101", type=str,
                      help="How far back to copy data.  Provide a date in 'yyyymmdd' format.")

    return parser.parse_args()


def convert_yyyymmdd_to_date(yyyymmdd):
    assert isinstance(yyyymmdd, str)

    return date(int(yyyymmdd[0:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8]))


def convert_date_to_yyyymmdd(a_date):
    assert isinstance(a_date, date)

    return str(a_date.year).zfill(4) + str(a_date.month).zfill(2) + str(a_date.day).zfill(2)


def daterange(start_date, end_date):
    assert isinstance(start_date, date) and isinstance(end_date, date)

    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def mongo_do_bulk_insert(target_collection, documents_to_insert):
    assert isinstance(target_collection, collection.Collection)
    assert isinstance(documents_to_insert, cursor.Cursor)

    print("Doing bulk insert of [%s] documents into destination [%s]" % (
        documents_to_insert.count(), target_collection.database.name + "." + target_collection.name))

    try:
        result = target_collection.insert_many(documents_to_insert)
    except BulkWriteError as bwe:
        pprint(bwe.details)
        exit()

    print(str(result))


def mongo_id_already_exists(object_id, target_collection):
    assert isinstance(object_id, objectid.ObjectId)
    assert isinstance(target_collection, collection.Collection)

    num_matching_documents = target_collection.find(filter={"_id": object_id}, limit=1).count()

    if num_matching_documents == 1:
        return True
    elif num_matching_documents == 0:
        return False
    else:
        print("ERROR: find() with limit of 1 returned [%d] documents." % num_matching_documents)
        exit()


def mongo_do_iterative_insert(target_collection, documents_to_insert):
    assert isinstance(target_collection, collection.Collection)
    assert isinstance(documents_to_insert, cursor.Cursor)

    print("Doing iterative insert of [%s] documents into destination [%s]" % (
        documents_to_insert.count(), target_collection.database.name + "." + target_collection.name))

    summary = {"already_exists_count": 0, "inserted_count": 0, "failed_count": 0, "failed_list": []}
    for document in documents_to_insert:
        object_id = document["_id"]
        if mongo_id_already_exists(object_id, target_collection):
            summary["already_exists_count"] += 1
            continue

        try:
            target_collection.insert_one(document)
        except:  # catch *all* exceptions
            e = sys.exc_info()[0]
            print("ERROR: %s" % e)
            summary["failed_count"] += 1
            summary["failed_list"].append(object_id)
        else:
            summary["inserted_count"] += 1

    if summary["inserted_count"] == documents_to_insert.count():
        print("Successfully inserted all [%s] documents" % documents_to_insert.count())
    else:
        print("Not all insertions succeeded. Insertion summary: %s" % str(summary))


if __name__ == "__main__":
    (options, args) = parse_args()
    start_date_str = options.start_date

    assert len(start_date_str) == 8, "Invalid date [%s]. Be sure to use 'yyyymmdd' format." % start_date_str
    start_date = convert_yyyymmdd_to_date(start_date_str)
    end_date = date.today()

    # connect to source and destination MongoDBs
    mongo_client_source = MongoClient(SOURCE_URI, ssl_ca_certs="./ca.pem")
    mongo_client_destination = MongoClient(DESTINATION_URI)

    # grab reference to the databases in the source and destination
    database_source = mongo_client_source["air_bnb"]
    database_destination = mongo_client_destination["air_bnb"]

    # get list of collection names
    collection_names_source = database_source.collection_names()

    # loop through every collection, copying from source to destination
    for this_date in daterange(start_date, end_date):
        collection_name = convert_date_to_yyyymmdd(this_date)

        if collection_name not in collection_names_source:
            print("[%s] not in source [%s].  Skipping." % (collection_name, database_source.name))
            continue

        collection_source = database_source[collection_name]

        documents = collection_source.find()  # get all documents in the collection

        # case 1: the collection needs to be created in the destination
        if collection_name not in database_destination.collection_names():
            database_destination.create_collection(collection_name)
            result = mongo_do_bulk_insert(database_destination[collection_name], documents)
            continue

        # case 2: the collection already exists in the destination
        assert collection_name in database_destination.collection_names()
        collection_destination = database_destination.get_collection(collection_name)
        mongo_do_iterative_insert(collection_destination, documents)


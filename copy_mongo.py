from base64 import standard_b64decode
from pprint import pprint
from pymongo import MongoClient
from optparse import OptionParser
from datetime import date

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


if __name__ == "__main__":
    (options, args) = parse_args()
    start_date_str = options.start_date

    assert len(start_date_str) == 8, "Invalid date [%s]. Be sure to use 'yyyymmdd' format." % start_date_str
    this_date = convert_yyyymmdd_to_date(start_date_str)

    mongo_client_source = MongoClient(SOURCE_URI, ssl_ca_certs="./ca.pem")
    mongo_client_destination = MongoClient(DESTINATION_URI)

    database_source = mongo_client_source["air_bnb"]
    database_destination = mongo_client_destination["air_bnb"]

    collection_names = database_source.collection_names()

    # loop through every collection in the source database
    while(this_date <= date.today()):
        collection_name = convert_date_to_yyyymmdd(this_date)

        print("Copying collection: %s" % collection_name)
        collection_source = database_source[collection_name]

        documents = collection_source.find()

        database_destination.create_collection(collection_name)
        collection_destination = database_destination.get_collection(collection_name)

        collection_destination.insert_many(documents)



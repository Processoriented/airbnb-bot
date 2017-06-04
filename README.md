# Airbnb Bot
## Getting Started
* Clone the repo.
* Run the multicontainer docker app:  
`$ docker-compose up --build -d`  
This consists of a MongoDB container and a Zeppelin container.  This will take a while the first time as it downloads the MongoDB and Zeppelin docker images.

* List the currently running docker containers:  
`$ docker ps`  
You should see `airbnbbot_zeppelin` and `airbnbbot_mongodb` listed.

* Confirm that Zeppelin is running by opening the web interface: http://localhost:8080/  
* Confirm that MongoDB is running:  
`$ docker exec -it airbnbbot_mongodb_1 mongo` (This opens a MongoDB shell on your MongoDB docker container)  
`> show dbs` (Show databases if you're curious)  
`> exit` (Exits the MongoDB shell and returns you to your terminal)  

## Copy data into local MongoDB docker container
The `copy_mongo.py` script will copy data from a remote MongoDB to your locally running MongoDB docker container.

NOTE: You will need to place a `ca.pem` file in the repo directory to connect to the remote MongoDB.  The `ca.pem` file will be provided to you by the workshop presenters.

Once you have the `ca.pem` file, you can start copying data, which will take a while.  For example, try:  
`$ python copy_mongo.py python copy_mongo.py --start-date=20170601` (Copies data from 6/1/2017 thru the current date)

For more usage info:  
`$ python copy_mongo.py --help`

## Play with Zeppelin
While you are waiting for the data copy to complete, you can begin playing with Zeppelin.

* Add the MongoDB Spark Connector to Zeppelin's Spark interpreter:
  * Go to the Zeppelin interprer menu: http://localhost:8080/#/interpreter
  * To be continued

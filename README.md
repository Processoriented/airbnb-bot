# Airbnb Bot
## Getting Started
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

* Add the MongoDB Spark Connector to Zeppelin's Spark interpreter:
  * Go to the Zeppelin interprer menu: http://localhost:8080/#/interpreter
  * To be continued

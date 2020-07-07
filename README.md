# RTDI Big Data S4HanaConnector 

_Capture changes in the ERP system and sent them to Kafka_

Source code available here: [github](https://github.com/rtdi/S4HanaConnector)


## Installation and testing

On any computer install the Docker Daemon - if it is not already - and download this docker image with

    docker pull rtdi/s4hanaconnector

Then start the image via docker run. For a quick test this command is sufficient

    docker run -d -p 80:8080 --rm --name s4hanaconnector  rtdi/s4hanaconnector

to expose a webserver at port 80 on the host running the container. Make sure to open the web page via the http prefix, as https needs more configuration.
For example [http://localhost:80/](http://localhost:80/) might do the trick of the container is hosted on the same computer.

The default login for this startup method is: **rtdi / rtdi!io**

The probably better start command is to mount two host directories into the container. In this example the host's /data/files contains all files to be loaded into Kafka and the /data/config is an (initially) empty directory where all settings made when configuring the connector will be stored permanently.

    docker run -d -p 80:8080 --rm -v /data/files:/data/ -v /data/config:/usr/local/tomcat/conf/security    --name s4hanaconnector  rtdi/s4hanaconnector


For proper start commands, especially https and security related, see the [ConnectorRootApp](https://github.com/rtdi/ConnectorRootApp) project, this application is based on.

  

### Connect the Pipeline to Kafka

The first step is to connect the application to a Kafka server, in this example Confluent Cloud.

<img src="https://github.com/rtdi/S4HanaConnector/raw/master/docs/media/DemoConnector-PipelineConfig.png" width="50%">


### Define a Connection

A Connection holds all information about the S/4Hana underlying Hana database. It connects via the Hana JDBC driver, not the ABAP layer.
The database use should be a new user which has read permissions on the S/4Hana tables and the permissions to create triggers on the tables.
Inside the own schema two control tables are created which are also the target of the triggers.

### Define the Schemas

The next step is to create the Avro Schemas for the SAP tables. 

### Create a Producer

A Producer stands for the process creating the data. One producer writes all data into a single topic to ensure transactional consistency.
For example a producer might capture all Material Management related data and produce that data in the topic MaterialManagement.


### Data content

The data in Kafka is a 1:1 copy if the ABPA table structure plus some extra metadata information about the record, e.g. the Hana transaction id.


## Licensing

This application is provided as dual license. For all users with less than 100'000 messages created per month, the application can be used free of charge and the code falls under a Gnu Public License. Users with more than 100'000 messages are asked to get a commercial license to support further development of this solution. The commercial license is on a monthly pay-per-use basis.


## Data protection and privacy

Every ten minutes the application does send the message statistics via a http call to a central server where the data is stored for information along with the public IP address (usually the IP address of the router). It is just a count which service was invoked how often, no information about endpoints, users, data or URL parameters. This information is collected to get an idea about the adoption.
To disable that, set the environment variable HANAAPPCONTAINERSTATISTICS=FALSE.
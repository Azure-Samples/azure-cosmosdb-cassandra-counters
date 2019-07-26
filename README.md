# Cosmos Cassandra Java samples

A reference Java app using data-stax drivers for using cosmos cassandra counters

##Counters from CQL
set SSL_VERSION=TLSv1_2 <br /> 
set SSL_VALIDATE=false<br />  
cqlsh.py testcassandra.cassandra.cosmos.azure.com 10350 -u <username> -p <key> <br /> 

testcassandra@cqlsh> CREATE KEYSPACE IF NOT EXISTS lprofile WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }; <br />  
testcassandra@cqlsh> CREATE TABLE IF NOT EXISTS lprofile.WebLogs(page_id uuid, page_name Text,insertion_time timestamp, page_count counter,PRIMARY KEY ((page_id,page_name), insertion_time)); <br />  
testcassandra@cqlsh> insert into lprofile.weblogs (page_id , page_name , insertion_time , page_count ) values(uuid(),’test.com’,dateof(now()),0) ; <br />  
testcassandra@cqlsh> update lprofile.weblogs set page_count = page_count + 1 where page_id =uuid() and page_name ='test.com' and insertion_time =dateof(now()); <br />  
testcassandra@cqlsh> select * from lprofile.weblogs; <br />  
testcassandra@cqlsh> update lprofile.weblogs set page_count = page_count + 1 where page_id = '1792ef1b-c297-4c08-9a11-805221ac2906' and page_name ='test.com' and insertion_time ='2019-07-25 00:05:36.089000+0000'; <br />  


## Development - Counters from Java datastax drivers

### Install dependencies

1. Clone the repository <br />  
2. Open a terminal in the local repo directory <br />  
3. git clone https://github.com/jyotsnaravikumar/azure-cosmos-cassandra-counter.git <br />  
4. cd cassandra-counter-demo <br />  
5. mvn clean install <br />  
6. java -cp target/cassandra-counter-demo.jar com.azure.cosmosdb.cassandra.examples.UserProfile <br />  


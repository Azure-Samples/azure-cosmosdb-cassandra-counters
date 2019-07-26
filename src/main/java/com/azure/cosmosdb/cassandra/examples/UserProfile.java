package com.azure.cosmosdb.cassandra.examples;

import com.azure.cosmosdb.cassandra.repository.UserRepository;
import com.azure.cosmosdb.cassandra.util.CassandraUtils;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Example class which will demonstrate following operations on Cassandra Database on CosmosDB
 * - Create Keyspace
 * - Create Table
 * - Insert Rows
 * - Select all data from a table
 * - Select a row from a table
 */
public class UserProfile {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserProfile.class);

    public static void main(String[] s) throws Exception {

        CassandraUtils utils = new CassandraUtils();
        Session cassandraSession = utils.getSession();

        try {
            UserRepository repository = new UserRepository(cassandraSession);

            //Create keyspace in cassandra database
            repository.createKeyspace();
            
            //Create table in cassandra database
            repository.createTable();

            //Update rows on counters table

            repository.executeQuery(1, "abc", "Dublin");
            repository.executeQuery(1, "abc", "Dublin");
            repository.executeQuery(1, "abc", "Dublin");
            repository.executeQuery(2, "def", "SF");
            repository.executeQuery(2, "def", "SF");
            repository.executeQuery(3, "fgh", "Sunnyvale");


            LOGGER.info("Select all users");
            repository.selectAllUsers();


        } finally {
            utils.close();
            LOGGER.info("Please delete your table after verifying the presence of the data in portal or from CQL");
        }
    }
}

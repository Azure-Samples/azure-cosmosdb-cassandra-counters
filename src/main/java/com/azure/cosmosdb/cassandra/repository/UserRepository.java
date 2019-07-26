package com.azure.cosmosdb.cassandra.repository;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.incr;

/**
 * This class gives implementations of create, delete table on Cassandra database
 * Insert & select data from the table
 */
public class UserRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserRepository.class);
    private Session session;

    public UserRepository(Session session) {
        this.session = session;
    }

    /**
     * Create keyspace uprofile in cassandra DB
     */
    public void createKeyspace() {
        final String query = "CREATE KEYSPACE IF NOT EXISTS userprofile WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 1 }";
        session.execute(query);
        LOGGER.info("Created keyspace 'userprofile'");
    }

    /**
     * Create user table in cassandra DB
     */
    public void createTable() {
        final String query = "CREATE TABLE IF NOT EXISTS userprofile.user (user_id int, user_name text, user_bcity text, user_count counter, PRIMARY KEY ((user_id,user_name), user_bcity))";
        session.execute(query);
        LOGGER.info("Created table 'user'");
    }


    /**
     * Select all rows from user table
     */
    public void selectAllUsers() {

        final String query = "SELECT * FROM userprofile.user";
        List<Row> rows = session.execute(query).all();

        for (Row row : rows) {
            LOGGER.info("Obtained row: {} | {} | {} ", row.getInt("user_id"), row.getString("user_name"), row.getString("user_bcity"), row.getLong("user_count"));
        }
    }

    /**
     * Select a row from user table
     *
     * @param user_name user_name
     */
    public void selectUser(String user_name) {
        final String query = "SELECT user_id, user_name, user_bcity FROM userprofile.user where user_name = 'Jyo'. ALLOW FILTERING";
        Row row = session.execute(query).one();

        LOGGER.info("Obtained row: {} | {} | {} ", row.getInt("user_id"), row.getString("user_name"), row.getString("user_bcity"), row.getLong("user_count"));
    }


    public void executeQuery(int id, String name, String city){
        Statement query = QueryBuilder.update("userprofile","user")
                .where(eq("user_id", id))
                .and(eq("user_name", name))
                .and(eq("user_bcity", city))
                .with(incr("user_count", 1)); // Use incr for counters

        session.execute(query);

    }




}
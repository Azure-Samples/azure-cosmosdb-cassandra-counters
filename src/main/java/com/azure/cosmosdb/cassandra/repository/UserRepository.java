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

    /**
     * Delete user table.
     */
    public void deleteTable() {
        final String query = "DROP TABLE IF EXISTS userprofile.user";
        session.execute(query);
    }

    /**
     * Insert a row into user table
     *
     * @param id   user_id
     * @param name user_name
     * @param city user_bcity
     */
    public void insertUser(PreparedStatement statement, int id, String name, String city) {
        BoundStatement boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(id, name, city));

    }

    /**
     * Insert a row into user table
     *
     * @param id   user_id
     * @param name user_name
     * @param city user_bcity
     */
    public void updateUser(PreparedStatement statement, int id, String name, String city) {
        BoundStatement boundStatement = new BoundStatement(statement);
        session.execute(boundStatement.bind(id, name, city));
    }


    /**
     * Create a PrepareStatement to insert a row to user table
     *
     * @return PreparedStatement
     */

    /*    public PreparedStatement prepareInsertStatement(){


        //final String insertStatement = "INSERT INTO  userprofile.user (user_id, user_name , user_bcity, user_count) VALUES (?,?,?,?)";
        //return session.prepare(insertStatement);
    }*/


    public void executeQuery(int id, String name, String city){
        Statement query = QueryBuilder.update("userprofile","user")
                .where(eq("user_id", id))
                .and(eq("user_name", name))
                .and(eq("user_bcity", city))
                .with(incr("user_count", 1)); // Use incr for counters

       /* QueryBuilder.insertInto("user").value("user_id", id)
                .value("user_name", name)
                .value("user_bcity", city);*/

        session.execute(query);

    }


    /**
     * Create a PrepareStatement to update a row to user table
     *
     * @return PreparedStatement
     */
    public PreparedStatement prepareUpdateStatement() {
        final String updateStatement = "UPDATE userprofile.user set user_count = user_count + 1 where user_id = ?, user_name = ? , user_bcity = ?) VALUES(?,?,?)";
        return session.prepare(updateStatement);
    }



}
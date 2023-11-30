import org.example.utils.DbUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class DbUtilsTest {

    private void insertUser(Connection connection, String email, String name) throws SQLException {
        String insertUserQuery = "INSERT INTO users (email, name) VALUES (?, ?)";
        try (var statement = connection.prepareStatement(insertUserQuery)) {
            statement.setString(1, email);
            statement.setString(2, name);
            statement.executeUpdate();
        }
    }

    @BeforeEach
    void deleteTestData(){
        String query = "drop table if exists test";
        String query2 = "Delete from users where email = 'john@example.com'";
        try {
            DbUtils.withConnection(connection -> {
                try (PreparedStatement statement = connection.prepareStatement(query)) {
                    statement.executeUpdate();

                }

                try (PreparedStatement statement = connection.prepareStatement(query2)) {
                    statement.executeUpdate();

                }
            });
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    void withConnectionSuccessTest() {

        String query = "CREATE TABLE test (id SERIAL PRIMARY KEY, email VARCHAR(255), name VARCHAR(255))";

        try {
            DbUtils.withConnection(connection -> {
                try (PreparedStatement statement = connection.prepareStatement(query)) {
                    statement.executeUpdate();
                }

                // Verify that the table was created
                boolean tableExists;
                try (ResultSet resultSet = connection.getMetaData().getTables(null, null, "test", null)) {
                    tableExists = resultSet.next();
                }
                assertTrue(tableExists, "Table 'test' does not exist");
            });
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }

    }

    @Test
    void withConnectionDuplicate() { //autocommit true, first record should be inserted

        assertThrows(RuntimeException.class, () -> {
            DbUtils.withConnection(connection -> {
                try {
                    insertUser(connection, "john@example.com", "John Doe");
                    // Intentionally insert the same user again, causing a duplicate key violation
                    insertUser(connection, "john@example.com", "John Doe");

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            });
        });
    }

    @Test
    void inTransactionWithoutResultSuccessTest() {

        String query = "CREATE TABLE test (id SERIAL PRIMARY KEY, email VARCHAR(255), name VARCHAR(255))";

        try {
            DbUtils.inTransactionWithoutResult(connection -> {
                try (PreparedStatement statement = connection.prepareStatement(query)) {
                    statement.executeUpdate();
                }

                // Verify that the table was created
                boolean tableExists;
                try (ResultSet resultSet = connection.getMetaData().getTables(null, null, "test", null)) {
                    tableExists = resultSet.next();
                }
                assertTrue(tableExists, "Table 'test' does not exist");
            });
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    void inTransactionWithoutResultDuplicateRollBack() {


        assertThrows(RuntimeException.class, () -> {
            DbUtils.inTransactionWithoutResult(connection -> {
                try {
                    insertUser(connection, "john@example.com", "John Doe");
                    // Intentionally insert the same user again, causing a duplicate key violation
                    insertUser(connection, "john@example.com", "John Doe");

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            });
        });
    }

    @Test
    void inTransactionSuccessTest() throws SQLException {
        String createTableQuery = "CREATE TABLE test (id SERIAL PRIMARY KEY, name VARCHAR(255))";
        String insertDataQuery = "INSERT INTO users (email, name) VALUES (?, ?)";

        /// Test creating a table in a transaction
        Integer result = DbUtils.inTransaction(connection -> {
            try (PreparedStatement createTableStatement = connection.prepareStatement(createTableQuery)) {
                createTableStatement.executeUpdate();
                // Assuming you want to return the number of rows affected
                return createTableStatement.getUpdateCount();
            }
        });

        assertEquals(0, result); // The number of rows affected by the create table statement


        // Test inserting data into a table in a transaction
        result = DbUtils.inTransaction(connection -> {
            try (PreparedStatement insertDataStatement = connection.prepareStatement(insertDataQuery)) {
                insertDataStatement.setString(1,"john@example.com");
                insertDataStatement.setString(2,"John Doe");
                insertDataStatement.executeUpdate();
                // Assuming you want to return the number of rows affected
                return insertDataStatement.getUpdateCount();
            }
        });

        assertEquals(1, result); // The number of rows affected by the insert data statement

    }

    @Test
    void inTransactionRollbackTest() throws SQLException {
        String insertDataQuery = "INSERT INTO users (email, name) VALUES (?, ?)";

        // Test creating a table and then intentionally failing with a duplicate email
        assertThrows(RuntimeException.class, () -> DbUtils.inTransaction(connection -> {
            try (PreparedStatement insertDataStatement = connection.prepareStatement(insertDataQuery)) {

                // Inserting a user with the same email should cause a duplicate key violation
                insertDataStatement.setString(1, "duplicate@example.com");
                insertDataStatement.setString(2, "John Doe");
                insertDataStatement.executeUpdate();

                // Inserting the same user again, triggering the exception and rollback
                insertDataStatement.setString(1, "duplicate@example.com");
                insertDataStatement.setString(2, "Another John");
                insertDataStatement.executeUpdate();

                // Assuming you want to return the number of rows affected
                return insertDataStatement.getUpdateCount();
            }
        }));
    }


}

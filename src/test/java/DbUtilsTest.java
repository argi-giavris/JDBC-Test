import org.example.utils.DbUtils;
import org.junit.jupiter.api.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;


public class DbUtilsTest {

    private void insertUser(Connection connection, String email, String name) throws SQLException {
        String insertUserQuery = "INSERT INTO test (email, name) VALUES (?, ?)";
        try (var statement = connection.prepareStatement(insertUserQuery)) {
            statement.setString(1, email);
            statement.setString(2, name);
            statement.executeUpdate();
        }
    }

    private boolean oneRowUserExists(String email) throws SQLException {
        String query = "SELECT COUNT(*) FROM test WHERE email = ?";
        try (Connection connection = DbUtils.dataSource.getConnection()) {
            try (var statement = connection.prepareStatement(query)) {
                statement.setString(1, email);
                try (var resultSet = statement.executeQuery()) {
                    resultSet.next();
                    return resultSet.getInt(1) == 1;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @BeforeAll
    static void createTestTable() throws SQLException {
        String query = "CREATE TABLE IF NOT EXISTS test (id SERIAL PRIMARY KEY, email VARCHAR(255) UNIQUE, name VARCHAR(255))";

        try {
            DbUtils.withConnection(connection -> {
                try (PreparedStatement statement = connection.prepareStatement(query)) {
                    statement.executeUpdate();
                }
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    void deleteTestData() {
        String query = "Delete from test";
        try {
            DbUtils.withConnection(connection -> {
                try (PreparedStatement statement = connection.prepareStatement(query)) {
                    statement.executeUpdate();
                }
            });
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @AfterEach
    void checkThreadLocalState() {
        DbUtils.checkThreadLocalState();
    }

    @AfterAll
    static void dropTestTable() {
        String query = "Drop table test";
        try {
            DbUtils.withConnection(connection -> {
                try (PreparedStatement statement = connection.prepareStatement(query)) {
                    statement.executeUpdate();
                }
            });
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    void withConnectionSuccessTest() throws SQLException {

        DbUtils.withConnection(connection -> {
            insertUser(connection, "john@example.com", "John Doe");
            insertUser(connection, "jin@example.com", "Jin Doe");
        });


        assertTrue(oneRowUserExists("john@example.com"));
        assertTrue(oneRowUserExists("jin@example.com"));

    }

    @Test
    void withConnectionDuplicate() throws SQLException {

        DbUtils.withConnection(connection -> insertUser(connection, "john@example.com", "John Doe"));

        assertThrows(RuntimeException.class, () -> DbUtils.withConnection(connection -> {
            insertUser(connection, "john@example.com", "John Doe");
        }));
        assertTrue(oneRowUserExists("john@example.com"));
    }

    @Test
    void withConnectionSameTransactionDuplicate() throws SQLException { //autocommit true, first record should be inserted

        assertThrows(RuntimeException.class, () -> DbUtils.withConnection(connection -> {
            insertUser(connection, "john@example.com", "John Doe");
            // Intentionally insert the same user again, causing a duplicate key violation
            insertUser(connection, "john@example.com", "John Doe");
        }));

        assertTrue(oneRowUserExists("john@example.com"));
    }

    @Test
    void withConnectionNestedTransactionDuplicate() throws SQLException { //autocommit true, first record should be inserted

        assertThrows(RuntimeException.class, () -> DbUtils.withConnection(connection -> {
            insertUser(connection, "john@example.com", "John Doe");
            DbUtils.withConnection(innerConnection -> {
                insertUser(innerConnection, "john@example.com", "John Doe");
            });
        }));
        assertTrue(oneRowUserExists("john@example.com"));
    }

    @Test
    void inTransactionWithoutResultSuccessTest() throws SQLException {

        DbUtils.inTransactionWithoutResult(connection -> {

            insertUser(connection, "john@example.com", "John Doe");
            insertUser(connection, "jin@example.com", "Jin Doe");

        });
        assertTrue(oneRowUserExists("john@example.com"));
        assertTrue(oneRowUserExists("jin@example.com"));
    }

    @Test
    void inTransactionWithoutResultDuplicateRollBack() throws SQLException {

        assertThrows(RuntimeException.class, () -> {
            DbUtils.inTransactionWithoutResult(connection -> {
                insertUser(connection, "john@example.com", "John Doe");
                // Intentionally insert the same user again, causing a duplicate key violation
                insertUser(connection, "john@example.com", "John Doe");
            });
        });
        assertFalse(oneRowUserExists("john@example.com"));
    }

    @Test
    void inTransactionWithoutResultNestedDuplicateRollBack() throws SQLException {

        assertThrows(RuntimeException.class, () -> {
            DbUtils.inTransactionWithoutResult(connection -> {
                insertUser(connection, "john@example.com", "John Doe");

                DbUtils.inTransactionWithoutResult(innerConnection -> {
                    try {
                        insertUser(innerConnection, "john@example.com", "John Doe");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
            });
        });
        assertFalse(oneRowUserExists("john@example.com"));
    }

    @Test
    void inTransactionSuccessTest() throws SQLException {

        DbUtils.ConnectionFunction<Integer> transactionFunction = connection -> {
            insertUser(connection, "john@example.com", "John Doe");
            insertUser(connection, "jin@example.com", "Jin Doe");
            return 2;
        };

        int rowsAffected = DbUtils.inTransaction(transactionFunction);

        assertTrue(oneRowUserExists("john@example.com"));
        assertTrue(oneRowUserExists("jin@example.com"));
        assertEquals(2, rowsAffected);

    }

    @Test
    void inTransactionRollbackTest() throws SQLException {
        DbUtils.ConnectionFunction<Void> transactionFunction = connection -> {
            insertUser(connection, "john@example.com", "John Doe");
            // Intentionally inserting the same user again, causing a duplicate key violation
            insertUser(connection, "john@example.com", "John Doe");
            return null;
        };

        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            DbUtils.inTransaction(transactionFunction);
        });

        assertTrue(exception.getCause() instanceof SQLException);
        assertTrue(exception.getMessage().contains("duplicate key value"));

        assertFalse(oneRowUserExists("john@example.com"));
    }

    @Test
    void nestedInTransactionRollbackTest() throws SQLException {

        DbUtils.ConnectionFunction<Void> outerTransaction = connection -> {
            insertUser(connection, "john@example.com", "John Doe");

            try {
                DbUtils.inTransaction(innerConnection -> {
                    insertUser(innerConnection, "john@example.com", "John Doe");
                    return null;
                });
                fail("Expected RuntimeException due to duplicate key violation in nested transaction");
            } catch (RuntimeException e) {
                assertTrue(e.getCause() instanceof SQLException);
                assertTrue(e.getMessage().contains("duplicate key value"));
                throw e; // Rethrow the exception to ensure it's caught by assertThrows
            }

            return null;
        };

        assertThrows(RuntimeException.class, () -> {
            DbUtils.inTransaction(outerTransaction);
        });

        assertFalse(oneRowUserExists("john@example.com"));
    }

}

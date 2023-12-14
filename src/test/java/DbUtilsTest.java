import org.example.utils.DbUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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

    @AfterAll
    static void dropTestTable(){
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

        try {
            DbUtils.withConnection(connection -> {
                try {
                    insertUser(connection, "john@example.com", "John Doe");
                    insertUser(connection, "jin@example.com", "Jin Doe");

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            });
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }

        assertTrue(oneRowUserExists("john@example.com"));
        assertTrue(oneRowUserExists("jin@example.com"));

    }

    @Test
    void withConnectionDuplicate() throws SQLException {

        try {
            DbUtils.withConnection(connection -> {
                try {
                    insertUser(connection, "john@example.com", "John Doe");

                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }

            });
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }

        assertThrows(RuntimeException.class, () -> DbUtils.withConnection(connection -> {
            try {
                insertUser(connection, "john@example.com", "John Doe");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }));
        assertTrue(oneRowUserExists("john@example.com"));
    }
    @Test
    void withConnectionSameTransactionDuplicate() throws SQLException { //autocommit true, first record should be inserted

        assertThrows(RuntimeException.class, () -> DbUtils.withConnection(connection -> {
            try {
                insertUser(connection, "john@example.com", "John Doe");
                // Intentionally insert the same user again, causing a duplicate key violation
                insertUser(connection, "john@example.com", "John Doe");
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }));
        assertTrue(oneRowUserExists("john@example.com"));
    }

    @Test
    void withConnectionNestedTransactionDuplicate() throws SQLException { //autocommit true, first record should be inserted

        assertThrows(RuntimeException.class, () -> DbUtils.withConnection(connection -> {
            try {
                insertUser(connection, "john@example.com", "John Doe");

                DbUtils.withConnection(innerConnection -> {
                    try {
                        insertUser(innerConnection, "john@example.com", "John Doe");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }));
        assertTrue(oneRowUserExists("john@example.com"));
    }

    @Test
    void inTransactionWithoutResultSuccessTest() throws SQLException {

        try {
            DbUtils.inTransactionWithoutResult(connection -> {
                try {
                    insertUser(connection, "john@example.com", "John Doe");
                    insertUser(connection, "jin@example.com", "Jin Doe");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            fail("Unexpected exception: " + e.getMessage());
        }
        assertTrue(oneRowUserExists("john@example.com"));
        assertTrue(oneRowUserExists("jin@example.com"));
    }

    @Test
    void inTransactionWithoutResultDuplicateRollBack() throws SQLException {


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
        assertFalse(oneRowUserExists("john@example.com"));
    }

    @Test
    void inTransactionWithoutResultNestedDuplicateRollBack() throws SQLException {


        assertThrows(RuntimeException.class, () -> {
            DbUtils.inTransactionWithoutResult(connection -> {
                try {
                    insertUser(connection, "john@example.com", "John Doe");

                    DbUtils.inTransactionWithoutResult(innerConnection -> {
                        try {
                            insertUser(innerConnection, "john@example.com", "John Doe");
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    });
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
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

        try {
            DbUtils.inTransaction(transactionFunction);
            fail("Expected RuntimeException due to duplicate key violation");
        } catch (RuntimeException e) {

            assertTrue(e.getCause() instanceof SQLException);
            assertTrue(e.getMessage().contains("duplicate key value"));
        }
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
                throw e;
            }
            return null;
        };

        try {
            DbUtils.inTransaction(outerTransaction);
            fail("Expected RuntimeException due to duplicate key violation in outer transaction");
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("duplicate key value"));
        }
        assertFalse(oneRowUserExists("john@example.com"));
    }

}

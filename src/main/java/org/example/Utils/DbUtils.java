package org.example.Utils;

import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DbUtils {
    public static DataSource dataSource;
    private static ThreadLocal<Boolean> transactionState = ThreadLocal.withInitial(() -> false);

    static {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setURL("jdbc:postgresql://localhost:5432/jdbc");
        ds.setUser("postgres");
        ds.setPassword("2108135592Ar");
        dataSource = ds;
    }

    @FunctionalInterface
    public interface ConnectionConsumer {
        void accept(Connection connection) throws Exception;
    }

    @FunctionalInterface
    public interface ConnectionFunction<T> {
        T apply(Connection connection) throws Exception;
    }

    public static void withConnection(Connection connection, ConnectionConsumer consumer) {
        try {
            if (!transactionState.get()) {
                transactionState.set(true);
            }
            consumer.accept(connection);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (!transactionState.get()) {
                transactionState.set(false);
            }
        }
    }

    public static void inTransactionWithoutResult(Connection connection, ConnectionConsumer consumer) throws SQLException {

        try {
            if (!transactionState.get()) {
                transactionState.set(true);
                connection.setAutoCommit(false);
            }

            consumer.accept(connection);
            connection.commit();

        } catch (Exception e) {
            handleRollbackAndException(connection, e);
        } finally {
            if (transactionState.get()) {
                transactionState.set(false);
            }
        }
    }

    public static <T> T inTransaction(Connection connection, ConnectionFunction<T> function) {
        try {
            if (!transactionState.get()) {
                transactionState.set(true);

            }
            connection.setAutoCommit(false);
            T result = function.apply(connection);
            connection.commit();
            return result;

        } catch (Exception e) {
            handleRollbackAndException(connection, e);
            return null;
        } finally {
            if (transactionState.get()) {
                transactionState.set(false);
            }
        }
    }

    private static void handleRollbackAndException(Connection connection, Exception e) {
        try {
            if (connection != null) {
                connection.rollback();
            }
        } catch (SQLException rollBackException) {
            rollBackException.printStackTrace();
        }
        e.printStackTrace();
    }

}

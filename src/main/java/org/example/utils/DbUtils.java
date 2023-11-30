package org.example.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import net.ttddyy.dsproxy.listener.logging.SLF4JLogLevel;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DbUtils {
    public static DataSource dataSource;
    private static final ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<>();
    private static final ThreadLocal<Integer> threadLocalDepthConnection = ThreadLocal.withInitial(() -> 0);
    private static Boolean commitInitialStatus;

    static {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/jdbc");
        config.setUsername("postgres");
        config.setPassword("2108135592Ar");
        //config.setMaximumPoolSize(5);

        dataSource = new HikariDataSource(config);
        dataSource = ProxyDataSourceBuilder
                .create(dataSource)
                .name("MyDataSource")
                .logQueryBySlf4j()
                .build();
    }

    @FunctionalInterface
    public interface ConnectionConsumer {
        void accept(Connection connection) throws Exception;
    }

    @FunctionalInterface
    public interface ConnectionFunction<T> {
        T apply(Connection connection) throws Exception;
    }

    public static void withConnection(ConnectionConsumer consumer) throws SQLException {
        Connection connection = getConnection();
        connection.setAutoCommit(true);

        try {
            consumer.accept(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (threadLocalDepthConnection.get() == 1) {
                try {
                    connection.close();
                    threadLocalConnection.set(null);
                } catch (SQLException e) {
                    throw  new SQLException(e);
                }
            }
            threadLocalDepthConnection.set(threadLocalDepthConnection.get() - 1);
        }
    }

    public static void inTransactionWithoutResult(ConnectionConsumer consumer) throws SQLException {
        Connection connection = getConnection();

        try {
            consumer.accept(connection);
            connection.commit();

        } catch (Exception e) {
            System.out.println("Depth " + threadLocalDepthConnection.get());
            connection.rollback();
            System.out.println("Roll back");
            throw new RuntimeException(e);

        } finally {

            if (threadLocalDepthConnection.get() == 1) {
                try {
                    connection.setAutoCommit(commitInitialStatus);
                    connection.close();
                    threadLocalConnection.set(null);
                } catch (SQLException e) {
                    throw  new SQLException(e);
                }
            }

            threadLocalDepthConnection.set(threadLocalDepthConnection.get() - 1);
        }
    }


    public static <T> T inTransaction(ConnectionFunction<T> function) throws SQLException {
        Connection connection = getConnection();
        try {

            T result = function.apply(connection);
            connection.commit();
            return result;

        } catch (Exception e) {
            System.out.println("Depth " + threadLocalDepthConnection.get());
            connection.rollback();
            System.out.println("Roll back");
            throw new RuntimeException(e);
        } finally {
            if (threadLocalDepthConnection.get() == 1) {
                try {
                    connection.setAutoCommit(commitInitialStatus);
                    connection.close();
                    threadLocalConnection.set(null);
                } catch (SQLException e) {
                    throw  new SQLException(e);
                }
            }
            threadLocalDepthConnection.set(threadLocalDepthConnection.get() - 1);

        }
    }

    private static Connection getConnection() throws SQLException {
        Connection connection = threadLocalConnection.get();
        if (connection == null) {
            connection = dataSource.getConnection();
            commitInitialStatus = connection.getAutoCommit();
            connection.setAutoCommit(false);
            threadLocalConnection.set(connection);
        }
        threadLocalDepthConnection.set(threadLocalDepthConnection.get() + 1);
        return connection;
    }




}

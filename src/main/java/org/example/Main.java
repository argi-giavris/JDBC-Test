package org.example;

import org.example.Models.User;
import org.example.Utils.DbUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.example.Utils.DbUtils.dataSource;

public class Main {

    User newUser = new User();
    public static void main(String[] args) throws SQLException {


        //example to test rollback
        try (Connection sharedConnection = dataSource.getConnection()) {
            DbUtils.inTransactionWithoutResult(sharedConnection, connection -> {
                try (PreparedStatement statement = connection.prepareStatement("Insert into users (email, name) VALUES (?, ?)")) {
                    statement.setString(1, "example9@email.com");
                    statement.setString(2, "Test Name6");
                    statement.executeUpdate();

                }

                DbUtils.inTransactionWithoutResult(sharedConnection, connection1 -> {
                    try (PreparedStatement statement = connection1.prepareStatement("Insert into users (email, name) VALUES (?, ?)")) {
                        statement.setString(1, "example9@email.com");
                        statement.setString(2, "Test Name6");
                        statement.executeUpdate();
                    }
                });
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (Connection sharedConnection = dataSource.getConnection()) {
            DbUtils.withConnection(sharedConnection, connection -> {
                try (PreparedStatement statement = connection.prepareStatement("Insert into users (email, name) VALUES (?, ?)")) {
                    statement.setString(1, "example10@email.com");
                    statement.setString(2, "Test Name10");
                    statement.executeUpdate();

                }
            });
        }

    }
}
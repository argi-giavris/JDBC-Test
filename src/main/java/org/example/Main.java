package org.example;

import org.example.Models.User;
import org.example.Utils.DbUtils;

import java.sql.*;

import static org.example.Utils.DbUtils.dataSource;

public class Main {


    public static void main(String[] args) throws SQLException {

        User newUser = new User("example9@email.com", "Test Name9");
        User newUser2 = new User("example10@email.com", "Test Name10");
        String query = "Insert into users (email, name) VALUES (?, ?)";

        //example to test rollback
        try (Connection sharedConnection = dataSource.getConnection()) {
            DbUtils.inTransactionWithoutResult(sharedConnection, connection -> {
                try (PreparedStatement statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
                    statement.setString(1, newUser.getEmail());
                    statement.setString(2, newUser.getName());
                    statement.executeUpdate();

                    try (ResultSet keys = statement.getGeneratedKeys()) {
                        if (keys.next()) {
                            newUser.setId(keys.getInt(1));
                        }
                    }

                }

                DbUtils.inTransactionWithoutResult(sharedConnection, connection1 -> {
                    try (PreparedStatement statement = connection1.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
                        statement.setString(1, newUser.getEmail());
                        statement.setString(2, newUser.getName());
                        statement.executeUpdate();

                        try (ResultSet keys = statement.getGeneratedKeys()) {
                            if (keys.next()) {
                                newUser.setId(keys.getInt(1));
                            }
                        }
                    }
                });
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }


        try (Connection sharedConnection = dataSource.getConnection()) {
            DbUtils.withConnection(sharedConnection, connection -> {
                try (PreparedStatement statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
                    statement.setString(1, newUser2.getEmail());
                    statement.setString(2, newUser2.getName());
                    statement.executeUpdate();

                    try (ResultSet keys = statement.getGeneratedKeys()) {
                        if (keys.next()) {
                            newUser2.setId(keys.getInt(1));
                        }
                    }

                }
            });
        }

    }
}
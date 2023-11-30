package org.example;

import org.example.models.User;
import org.example.utils.DbUtils;

import java.sql.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.LoggerFactory;

import static org.example.utils.DbUtils.dataSource;

public class Main {


    public static void main(String[] args) throws SQLException {

        User newUser = new User("example9@email.com", "Test Name9");
        User newUser3 = new User("example10@email.com", "Test Name9");
        String query = "Insert into users (email, name) VALUES (?, ?)";
        //String query2 = "Insert into route (name, grade) VALUES (?, ?)";


        //example to test rollback
        try {
            DbUtils.inTransactionWithoutResult(connection -> {
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

                DbUtils.inTransactionWithoutResult(connection1 -> {
                    try (PreparedStatement statement = connection1.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

                        statement.setString(1, newUser3.getEmail());
                        statement.setString(2, newUser3.getName());
                        statement.executeUpdate();

                        try (ResultSet keys = statement.getGeneratedKeys()) {
                            if (keys.next()) {
                                newUser3.setId(keys.getInt(1));
                            }
                        }

                    }
                });
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

//        DbUtils.withConnection(connection -> {
//            try (PreparedStatement statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
//                statement.setString(1, newUser2.getEmail());
//                statement.setString(2, newUser2.getName());
//                statement.executeUpdate();
//
//                try (ResultSet keys = statement.getGeneratedKeys()) {
//                    if (keys.next()) {
//                        newUser2.setId(keys.getInt(1));
//                    }
//                }
//
//            }
//        });


}

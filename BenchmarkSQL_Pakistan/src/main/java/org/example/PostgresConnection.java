// src/main/java/org/example/PostgresConnection.java
package org.example;

import io.github.cdimascio.dotenv.Dotenv;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresConnection {



    public static Connection getConnection() throws SQLException {
        String user = "admin";
        String password = "password";
        String url = "jdbc:postgresql://localhost:5432/pakistan";

        return DriverManager.getConnection(url, user, password);
    }
}
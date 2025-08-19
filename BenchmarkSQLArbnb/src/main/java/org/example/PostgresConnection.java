// src/main/java/org/example/PostgresConnection.java
package org.example;

import io.github.cdimascio.dotenv.Dotenv;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgresConnection {

    private static final Dotenv dotenv = Dotenv.load();

    public static Connection getConnection() throws SQLException {
        String user = dotenv.get("USER");
        String password = dotenv.get("PASSWORD");
        String url = dotenv.get("DB_URL");

        return DriverManager.getConnection(url, user, password);
    }
}
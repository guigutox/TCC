package org.example.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class QueryExecutor {
    private final Connection connection;

    public QueryExecutor(Connection connection) {
        this.connection = connection;
    }

    public void execute(String sql) throws Exception {
        long start = System.currentTimeMillis();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
               // System.out.println(rs.getString(1));
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("Query executada em " + (end - start) + " ms");
    }

}
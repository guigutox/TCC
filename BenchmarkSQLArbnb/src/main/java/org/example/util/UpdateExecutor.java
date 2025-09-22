package org.example.util;

import java.sql.Connection;
import java.sql.Statement;

public class UpdateExecutor {
    private final Connection connection;

    public UpdateExecutor(Connection connection) {
        this.connection = connection;
    }

    public void executeUpdate(String sql) throws Exception {
        long start = System.currentTimeMillis();
        try (Statement stmt = connection.createStatement()) {
            int affectedRows = stmt.executeUpdate(sql);
            System.out.println("Linhas afetadas: " + affectedRows);
        }
        long end = System.currentTimeMillis();
        System.out.println("Tempo de execução: " + (end - start) + " ms");
    }
}

// src/main/java/org/example/util/DeleteExecutor.java
package org.example.util;

import java.sql.Connection;
import java.sql.Statement;

public class DeleteExecutor {
    public static long deleteAll(Connection conn) throws Exception {
        long start = System.currentTimeMillis();
        try (Statement stmt = conn.createStatement()) {
            int count2 = stmt.executeUpdate("DELETE FROM reviews");
            System.out.println("Antes do DELETE listings...");
            int count = stmt.executeUpdate("DELETE FROM listings");
            System.out.println("Depois do DELETE listings. Registros deletados: " + count);
        } catch (Exception e) {
            System.out.println("Erro ao deletar: " + e.getMessage());
            throw e;
        }
        long end = System.currentTimeMillis();
        return end - start;
    }
}

package org.example;

import java.sql.*;

public class Main {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/pakistan";
    private static final String DB_USER = "admin";
    private static final String DB_PASSWORD = "password";

    public static void main(String[] args) {
        String filePath = "./src/main/resources/data/PakistanLargestEcommerceDataset.csv";

        PakistanImporter importer = new PakistanImporter();
        importer.createTableIfNotExists();
        long duration = importer.importData(filePath);
        System.out.println("⏱️ Tempo total de inserção: " + duration + " ms");

        long tempoBusca = realizarQuery("EXPLAIN ANALYZE SELECT * FROM pakistan_orders");
        System.out.println("Tempo de execução para busca: " + tempoBusca + "ms");

    }
    private static long realizarQuery(String query) {

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement statement = connection.createStatement()) {
            long startTime = System.nanoTime();
            ResultSet rs = statement.executeQuery(query);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;

            try {
                while (rs != null && rs.next()) {
                    //System.out.println("Anime: " + rs.getString("name") + " | Score: " + rs.getFloat("score"));
                    System.out.println(rs.getString("QUERY PLAN"));
                }
                return duration;
            } catch (SQLException e) {
                System.err.println("❌ Erro ao processar o resultado: " + e.getMessage());
            }

        } catch (SQLException e) {
            System.err.println("❌ Erro ao executar a query: " + e.getMessage());

        }
        return 0;
    }
}

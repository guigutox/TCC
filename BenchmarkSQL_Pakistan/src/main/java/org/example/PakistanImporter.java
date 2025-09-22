package org.example;

import com.opencsv.*;
import java.io.FileReader;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PakistanImporter {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/pakistan";
    private static final String DB_USER = "admin";
    private static final String DB_PASSWORD = "password";

    public void createTableIfNotExists() {
        String sql = """
            CREATE TABLE IF NOT EXISTS pakistan_orders (
                item_id INT PRIMARY KEY,
                status VARCHAR(50),
                created_at DATE,
                sku TEXT,
                price NUMERIC,
                qty_ordered INT,
                grand_total NUMERIC,
                increment_id BIGINT,
                category_name_1 TEXT,
                sales_commission_code TEXT,
                discount_amount NUMERIC,
                payment_method VARCHAR(50),
                working_date DATE,
                bi_status TEXT,
                mv NUMERIC,
                year INT,
                month INT,
                customer_since DATE,
                my TEXT,
                fy TEXT,
                customer_id INT,
                column1 TEXT,
                _1 TEXT,
                _2 TEXT,
                _3 TEXT,
                _4 TEXT
            );
        """;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("✅ Tabela verificada/criada.");
        } catch (SQLException e) {
            System.err.println("❌ Erro criando tabela: " + e.getMessage());
        }
    }

    public long importData(String filePath) {
        long start = System.currentTimeMillis();
        int count = 0;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             CSVReader reader = new CSVReaderBuilder(new FileReader(filePath))
                     .withCSVParser(new CSVParserBuilder().withSeparator(';').build())
                     .build()) {

            conn.setAutoCommit(false);

            String insert = """
                INSERT INTO pakistan_orders VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                ) ON CONFLICT (item_id) DO NOTHING
            """;

            PreparedStatement stmt = conn.prepareStatement(insert);

            String[] header = reader.readNext(); // pula cabeçalho

            String[] row;
            while ((row = reader.readNext()) != null) {
                if (row.length < 26) continue;

                try {
                    stmt.setInt(1, parseInt(row[0]));
                    stmt.setString(2, row[1]);
                    stmt.setDate(3, parseDate(row[2]));
                    stmt.setString(4, row[3]);
                    stmt.setBigDecimal(5, parseDecimal(row[4]));
                    stmt.setInt(6, parseInt(row[5]));
                    stmt.setBigDecimal(7, parseDecimal(row[6]));
                    stmt.setLong(8, parseLong(row[7]));
                    stmt.setString(9, row[8]);
                    stmt.setString(10, row[9]);
                    stmt.setBigDecimal(11, parseDecimal(row[10]));
                    stmt.setString(12, row[11]);
                    stmt.setDate(13, parseDate(row[12]));
                    stmt.setString(14, row[13]);
                    stmt.setBigDecimal(15, parseDecimal(row[14]));
                    stmt.setInt(16, parseInt(row[15]));
                    stmt.setInt(17, parseInt(row[16]));
                    stmt.setDate(18, parseDate(row[17]));
                    stmt.setString(19, row[18]);
                    stmt.setString(20, row[19]);
                    stmt.setInt(21, parseInt(row[20]));
                    stmt.setString(22, row[21]);
                    stmt.setString(23, row[22]);
                    stmt.setString(24, row[23]);
                    stmt.setString(25, row[24]);
                    stmt.setString(26, row[25]);

                    stmt.addBatch();
                    count++;

                    if (count % 500 == 0) {
                        stmt.executeBatch();
                        conn.commit();
                        System.out.println(" Inseridos até agora: " + count);
                    }

                } catch (Exception e) {
                    System.err.println(" Linha ignorada: " + String.join(";", row));
                }
            }

            stmt.executeBatch();
            conn.commit();

        } catch (Exception e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        System.out.println("✅ Total de linhas inseridas: " + count);
        return end - start;
    }

    private java.sql.Date parseDate(String s) {
        try {
            if (s == null || s.trim().isEmpty()) return null;
            Date date = new SimpleDateFormat("dd/MM/yyyy").parse(s.trim());
            return new java.sql.Date(date.getTime());
        } catch (Exception e) {
            return null;
        }
    }

    private Integer parseInt(String s) {
        try {
            return s == null || s.isBlank() ? null : Integer.parseInt(s.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private Long parseLong(String s) {
        try {
            return s == null || s.isBlank() ? null : Long.parseLong(s.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private java.math.BigDecimal parseDecimal(String s) {
        try {
            return s == null || s.isBlank() ? null :
                    new java.math.BigDecimal(s.replace(",", ".").trim());
        } catch (Exception e) {
            return null;
        }
    }

    public void clearTable() {
        String sql = "DELETE FROM pakistan_orders";
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {
            long start = System.currentTimeMillis();
            stmt.execute(sql);
            long end = System.currentTimeMillis();
            System.out.println("⏱️ Tempo para limpar tabela: " + (end - start) + " ms");
            System.out.println("✅ Tabela limpa com sucesso.");
        } catch (SQLException e) {
            System.err.println("❌ Erro ao limpar tabela: " + e.getMessage());
        }
    }

    public long executeUpdate(String sql) {
        long start = System.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(true);
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("⏱️ Tempo para executar update: " + (end - start) + " ms");
        return end - start;
    }

}

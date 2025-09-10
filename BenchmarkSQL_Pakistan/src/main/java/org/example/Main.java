// src/main/java/org/example/Main.java
package org.example;

import java.sql.*;

import io.github.cdimascio.dotenv.Dotenv;
import org.example.QueryExecutor;

public class Main {
    private static Dotenv dotenv = Dotenv.load();
    private static final String DB_USER = dotenv.get("USER");
    private static final String DB_PASSWORD = dotenv.get("PASSWORD");
    private static final String DB_URL = dotenv.get("DB_URL");
    private static final String CSV_PATH = "./src/main/resources/data/PakistanLargestEcommerceDataset.csv";

    public static void main(String[] args) throws Exception {
        PakistanImporter importer = new PakistanImporter();
        java.util.Scanner scanner = new java.util.Scanner(System.in);

        while (true) {
            System.out.println("\nMenu:");
            System.out.println("1 - Criar tabela");
            System.out.println("2 - Importar dados");
            System.out.println("3 - Limpar tabela");
            System.out.println("0 - Sair");
            System.out.print("Escolha uma opção: ");
            int opcao = scanner.nextInt();
            scanner.nextLine(); // consumir quebra de linha

            switch (opcao) {
                case 1:
                    importer.createTableIfNotExists();
                    break;
                case 2:
                    long tempo = importer.importData(CSV_PATH);
                    System.out.println("Tempo de importação: " + tempo + " ms");
                    break;
                case 3:
                    importer.clearTable();
                    break;
                case 4:
                    Connection conn = PostgresConnection.getConnection();
                    QueryExecutor executor = new QueryExecutor(conn);
                    // 1) Contagem de pedidos por status
// Q1: Contagem de pedidos por status
                    executor.execute("SELECT status, COUNT(*) AS total_orders FROM pakistan_orders GROUP BY status ORDER BY total_orders DESC;");

// 2) Contagem de pedidos por método de pagamento
// Q2: Pedidos por payment_method
                    executor.execute("SELECT payment_method, COUNT(*) AS total_orders FROM pakistan_orders GROUP BY payment_method ORDER BY total_orders DESC;");

// 3) Contagem de pedidos por ano/mês
// Q3: Pedidos por ano/mês
                    executor.execute("SELECT year, month, COUNT(*) AS total_orders FROM pakistan_orders GROUP BY year, month ORDER BY year, month;");

// 4) Top 10 clientes por faturamento
// Q4: Top 10 clientes por faturamento
                    executor.execute("SELECT customer_id AS customer, SUM(grand_total) AS total_revenue FROM pakistan_orders GROUP BY customer_id ORDER BY total_revenue DESC LIMIT 10;");

// 5) Top 10 SKUs por quantidade
// Q5: Top 10 SKUs por quantidade
                    executor.execute("SELECT sku, SUM(qty_ordered) AS total_qty FROM pakistan_orders GROUP BY sku ORDER BY total_qty DESC LIMIT 10;");

// 6) Top 5 categorias por faturamento
// Q6: Top 5 categorias por faturamento
                    executor.execute("SELECT category_name_1, SUM(grand_total) AS total_revenue FROM pakistan_orders GROUP BY category_name_1 ORDER BY total_revenue DESC LIMIT 5;");

// 7) Ticket médio por status
// Q7: Ticket médio por status
                    executor.execute("SELECT status, AVG(grand_total) AS avg_ticket FROM pakistan_orders GROUP BY status ORDER BY avg_ticket DESC;");

// 8) Média de quantidade por SKU (Top 10)
// Q8: Média de qty por SKU (top 10)
                    executor.execute("SELECT sku, AVG(qty_ordered) AS avg_qty FROM pakistan_orders GROUP BY sku ORDER BY avg_qty DESC LIMIT 10;");

// 9) Média de MV por FY
// Q9: Média de MV por FY
                    executor.execute("SELECT payment_method, COUNT(*) AS total_orders, SUM(grand_total) AS total_revenue FROM pakistan_orders WHERE grand_total IS NOT NULL GROUP BY payment_method ORDER BY total_revenue DESC;");

// 10) Pedidos por dia da semana
// Q10: Pedidos por dia da semana
                    executor.execute("SELECT TO_CHAR(working_date, 'Day') AS weekday, COUNT(*) AS total_orders FROM pakistan_orders WHERE working_date IS NOT NULL GROUP BY weekday ORDER BY total_orders DESC;");

// 11) Faturamento por ano
// Q11: Faturamento por ano
                    executor.execute("SELECT year, SUM(grand_total) AS total_revenue FROM pakistan_orders WHERE year IS NOT NULL GROUP BY year ORDER BY year;");

// 12) Desconto total por categoria
// Q12: Desconto total por categoria
                    executor.execute("SELECT category_name_1, SUM(discount_amount) AS total_discount FROM pakistan_orders GROUP BY category_name_1 ORDER BY total_discount DESC;");

// 13) Taxa de cancelamento por ano
// Q13: Taxa de cancelamento por ano
                    executor.execute("SELECT year, 100.0 * SUM(CASE WHEN status = 'canceled' THEN 1 ELSE 0 END) / COUNT(*) AS cancel_rate FROM pakistan_orders WHERE year IS NOT NULL GROUP BY year ORDER BY year;");

// 14) Receita COD vs não-COD por ano
// Q14: Receita COD vs não-COD por ano
                    executor.execute("SELECT year, SUM(CASE WHEN payment_method = 'cod' THEN grand_total ELSE 0 END) AS cod_revenue, SUM(CASE WHEN payment_method <> 'cod' THEN grand_total ELSE 0 END) AS non_cod_revenue FROM pakistan_orders WHERE year IS NOT NULL GROUP BY year ORDER BY year;");

// 15) Top 5 categorias por preço médio
// Q15: Top 5 categorias por preço médio
                    executor.execute("SELECT category_name_1, AVG(price) AS avg_price FROM pakistan_orders WHERE category_name_1 IS NOT NULL GROUP BY category_name_1 ORDER BY avg_price DESC LIMIT 5;");

// 16) Top 5 clientes por pedidos e receita
// Q16: Top 5 clientes por pedidos e receita
                    executor.execute("SELECT customer_id AS customer, COUNT(*) AS orders, SUM(grand_total) AS revenue FROM pakistan_orders GROUP BY customer_id ORDER BY revenue DESC, orders DESC LIMIT 5;");

// 17) Top 5 SKUs por faturamento em 2017
// Q17: Top 5 SKUs por faturamento (2017)
                    executor.execute("SELECT sku, SUM(grand_total) AS total_revenue FROM pakistan_orders WHERE year = 2017 GROUP BY sku ORDER BY total_revenue DESC LIMIT 5;");

// 18) Receita mensal de 2018
// Q18: Receita mensal de 2018
                    executor.execute("SELECT month, SUM(grand_total) AS total_revenue FROM pakistan_orders WHERE year = 2018 GROUP BY month ORDER BY month;");

// 19) Taxa média de desconto por método de pagamento
// Q19: Taxa média de desconto por método
                    executor.execute("SELECT payment_method, AVG(discount_amount) AS avg_discount FROM pakistan_orders GROUP BY payment_method ORDER BY avg_discount DESC;");

// 20) Top 5 clientes por ticket médio (>=2 pedidos)
// Q20: Top 5 clientes por ticket médio (>=2 pedidos)
                    executor.execute("SELECT customer_id AS customer, AVG(grand_total) AS avg_ticket, COUNT(*) AS total_orders FROM pakistan_orders GROUP BY customer_id HAVING COUNT(*) >= 2 ORDER BY avg_ticket DESC LIMIT 5;");

                    break;
                case 0:
                    System.out.println("Saindo...");
                    return;
                default:
                    System.out.println("Opção inválida.");
            }
        }
    }
}
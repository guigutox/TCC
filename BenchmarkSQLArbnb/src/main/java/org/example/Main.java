package org.example;

import org.example.models.Listing;
import org.example.repository.ListingRepositoryPostgres;
import org.example.util.CsvReader;
import org.example.util.DatabaseSetup;
import org.example.util.QueryExecutor;

import java.sql.Connection;
import java.util.Map;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Digite 1 para inserir dados ou 2 para executar queries:");
        int opcao = scanner.nextInt();
        scanner.nextLine(); // consumir quebra de linha

        String listingsCsv = "./src/main/resources/data/listings.csv";
        String reviewsCsv = "./src/main/resources/data/reviews.csv";

        try (Connection conn = PostgresConnection.getConnection()) {
            if (opcao == 1) {
                // Inserção
                Map<String, Listing> listings = CsvReader.loadListings(listingsCsv);
                CsvReader.loadReviews(reviewsCsv, listings);

                DatabaseSetup.createTablesIfNotExists(conn);
                ListingRepositoryPostgres repository = new ListingRepositoryPostgres(conn);
                repository.saveAll(listings);

                System.out.println("Inserção concluída! Total de listings: " + listings.size());
            } else if (opcao == 2) {
                // Execução de queries
                QueryExecutor executor = new QueryExecutor(conn);
// 1) Contagem de listings por tipo de quarto
                executor.execute("SELECT room_type, COUNT(*) AS total " +
                        "FROM listings GROUP BY room_type ORDER BY total DESC;");

// 2) Média de preço por bairro
                executor.execute(" SELECT neighbourhood, AVG(price) AS avg_price " +
                        "FROM listings GROUP BY neighbourhood ORDER BY avg_price DESC;");

// 3) Top 10 hosts com mais listings
                executor.execute("SELECT host_id, COUNT(*) AS total_listings " +
                        "FROM listings GROUP BY host_id ORDER BY total_listings DESC LIMIT 10;");

// 4) Top 5 bairros mais caros (preço médio)
                executor.execute("SELECT neighbourhood, AVG(price) AS avg_price " +
                        "FROM listings GROUP BY neighbourhood ORDER BY avg_price DESC LIMIT 5;");

// 5) Estatísticas de preço por tipo de quarto (min, max, avg)
                executor.execute("SELECT room_type, MIN(price) AS min_price, MAX(price) AS max_price, AVG(price) AS avg_price " +
                        "FROM listings GROUP BY room_type;");

// 6) Contagem de listings com preço < 50
                executor.execute("SELECT COUNT(*) AS total " +
                        "FROM listings WHERE price < 50;");

// 7) Hosts com mais de 50 listings
                executor.execute("SELECT host_id, COUNT(*) AS total_listings " +
                        "FROM listings GROUP BY host_id HAVING COUNT(*) > 50 ORDER BY total_listings DESC;");

// 8) Distribuição de preços em faixas
                executor.execute("SELECT " +
                        "CASE " +
                        "WHEN price < 50 THEN '0-50' " +
                        "WHEN price < 100 THEN '50-100' " +
                        "WHEN price < 200 THEN '100-200' " +
                        "WHEN price < 500 THEN '200-500' " +
                        "WHEN price < 1000 THEN '500-1000' " +
                        "ELSE '>=1000' END AS faixa_preco, COUNT(*) AS total " +
                        "FROM listings GROUP BY faixa_preco ORDER BY total DESC;");

// 9) Reviews por ano
                executor.execute("SELECT EXTRACT(YEAR FROM date) AS ano, COUNT(*) AS total_reviews " +
                        "FROM reviews GROUP BY ano ORDER BY ano ASC;");

// 10) Top 10 listings por quantidade de reviews
                executor.execute("SELECT l.id, l.name, COUNT(r.id) AS reviews_count " +
                        "FROM listings l LEFT JOIN reviews r ON l.id = r.listing_id " +
                        "GROUP BY l.id, l.name ORDER BY reviews_count DESC LIMIT 10;");

// 11) Média do tamanho dos comentários por listing (top 10)
                executor.execute("SELECT l.id, l.name, AVG(LENGTH(r.comments)) AS avg_comment_len " +
                        "FROM listings l JOIN reviews r ON l.id = r.listing_id " +
                        "GROUP BY l.id, l.name ORDER BY avg_comment_len DESC LIMIT 10;");

// 12) Top 10 reviewers mais ativos
                executor.execute("SELECT reviewer_id, reviewer_name, COUNT(*) AS total_reviews " +
                        "FROM reviews GROUP BY reviewer_id, reviewer_name ORDER BY total_reviews DESC LIMIT 10;");

// 13) Bairros com mais reviews
                executor.execute("SELECT l.neighbourhood, COUNT(r.id) AS total_reviews " +
                        "FROM listings l JOIN reviews r ON l.id = r.listing_id " +
                        "GROUP BY l.neighbourhood ORDER BY total_reviews DESC LIMIT 10;");

// 14) Média do campo number_of_reviews por bairro
                executor.execute("SELECT neighbourhood, AVG(number_of_reviews) AS avg_reviews " +
                        "FROM listings GROUP BY neighbourhood ORDER BY avg_reviews DESC;");

// 15) Média de reviews_per_month por bairro
                executor.execute("SELECT neighbourhood, AVG(reviews_per_month) AS avg_reviews_per_month " +
                        "FROM listings GROUP BY neighbourhood ORDER BY avg_reviews_per_month DESC;");

// 16) Disponibilidade média (availability_365) por bairro
                executor.execute("SELECT neighbourhood, AVG(availability_365) AS avg_availability " +
                        "FROM listings GROUP BY neighbourhood ORDER BY avg_availability DESC;");

// 17) Total de reviews por host (somando reviews embutidos)
                executor.execute("SELECT l.host_id, COUNT(r.id) AS total_reviews " +
                        "FROM listings l JOIN reviews r ON l.id = r.listing_id " +
                        "GROUP BY l.host_id ORDER BY total_reviews DESC LIMIT 10;");

// 18) Última data de review por listing
                executor.execute("SELECT l.id, l.name, MAX(r.date) AS last_review_date " +
                        "FROM listings l JOIN reviews r ON l.id = r.listing_id " +
                        "GROUP BY l.id, l.name ORDER BY last_review_date DESC LIMIT 10;");

// 19) Listings com pelo menos 10 reviews
                executor.execute("SELECT l.id, l.name, COUNT(r.id) AS reviews_count " +
                        "FROM listings l JOIN reviews r ON l.id = r.listing_id " +
                        "GROUP BY l.id, l.name HAVING COUNT(r.id) >= 10 ORDER BY reviews_count DESC LIMIT 20;");

// 20) Top 10 listings mais caros e com mais reviews
                executor.execute("SELECT l.id, l.name, l.price, COUNT(r.id) AS reviews_count " +
                        "FROM listings l JOIN reviews r ON l.id = r.listing_id " +
                        "GROUP BY l.id, l.name, l.price ORDER BY l.price DESC, reviews_count DESC LIMIT 10;");
// Q21: Média de preço por bairro + tipo de quarto
                executor.execute("SELECT neighbourhood, room_type, AVG(price) AS avg_price " +
                        "FROM listings GROUP BY neighbourhood, room_type ORDER BY avg_price DESC;");


// Q22: Top 5 room types com maior preço médio (listings com >=5 reviews)
                executor.execute(
                        "SELECT room_type, AVG(price) AS avg_price, COUNT(*) AS total_listings " +
                                "FROM listings " +
                                "WHERE number_of_reviews >= 5 " +
                                "GROUP BY room_type " +
                                "ORDER BY avg_price DESC " +
                                "LIMIT 5;"
                );


// Q23: Hosts com maior preço médio entre seus listings
                executor.execute("SELECT host_id, AVG(price) AS avg_price, COUNT(*) AS total_listings " +
                        "FROM listings GROUP BY host_id HAVING COUNT(*) >= 5 " +
                        "ORDER BY avg_price DESC LIMIT 10;");

// Q24: Ranking de listings por número de reviews
                executor.execute("SELECT l.id, l.name, COUNT(r.id) AS reviews_count " +
                        "FROM listings l LEFT JOIN reviews r ON l.id = r.listing_id " +
                        "GROUP BY l.id, l.name ORDER BY reviews_count DESC LIMIT 20;");

// Q25: Mediana de preço por bairro
                executor.execute("SELECT neighbourhood, " +
                        "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price " +
                        "FROM listings GROUP BY neighbourhood ORDER BY median_price DESC;");




            } else {
                System.out.println("Opção inválida.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
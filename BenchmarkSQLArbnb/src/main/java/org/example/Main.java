package org.example;

import org.example.models.Listing;
import org.example.repository.ListingRepositoryPostgres;
import org.example.util.CsvReader;
import org.example.util.DatabaseSetup;

import java.sql.Connection;
import java.util.Map;

public class Main {
    public static void main(String[] args) {

        String listingsCsv = "./src/main/resources/data/listings.csv";
        String reviewsCsv = "./src/main/resources/data/reviews.csv";

        // Ler CSVs
        Map<String, Listing> listings = CsvReader.loadListings(listingsCsv);
        CsvReader.loadReviews(reviewsCsv, listings);

        try (Connection conn = PostgresConnection.getConnection()) {

            DatabaseSetup.createTablesIfNotExists(conn); // cria listings e reviews

            ListingRepositoryPostgres repository = new ListingRepositoryPostgres(conn);
            repository.saveAll(listings);

            System.out.println("Inserção concluída! Total de listings: " + listings.size());

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

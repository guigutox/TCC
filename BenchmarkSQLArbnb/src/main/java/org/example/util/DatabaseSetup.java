package org.example.util;

import java.sql.Connection;
import java.sql.Statement;

public class DatabaseSetup {

    public static void createTablesIfNotExists(Connection conn) {
        try (Statement stmt = conn.createStatement()) {

            String createListings = """
                CREATE TABLE IF NOT EXISTS listings (
                    id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    host_id VARCHAR,
                    host_name VARCHAR,
                    neighbourhood_group VARCHAR,
                    neighbourhood VARCHAR,
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    room_type VARCHAR,
                    price INTEGER,
                    minimum_nights INTEGER,
                    number_of_reviews INTEGER,
                    last_review DATE,
                    reviews_per_month DOUBLE PRECISION,
                    calculated_host_listings_count INTEGER,
                    availability_365 INTEGER,
                    number_of_reviews_ltm INTEGER,
                    license VARCHAR
                );
            """;

            String createReviews = """
                CREATE TABLE IF NOT EXISTS reviews (
                    id VARCHAR PRIMARY KEY,
                    listing_id VARCHAR REFERENCES listings(id),
                    date DATE,
                    reviewer_id VARCHAR,
                    reviewer_name VARCHAR,
                    comments TEXT
                );
            """;

            stmt.execute(createListings);
            stmt.execute(createReviews);

            System.out.println("Tabelas criadas (ou já existiam).");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

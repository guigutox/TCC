package org.example.repository;

import org.example.models.Listing;
import org.example.models.Review;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;

public class ListingRepositoryPostgres {

    private final Connection conn;
    private final int batchSize = 500;

    public ListingRepositoryPostgres(Connection conn) {
        this.conn = conn;
    }

    public void saveAll(Map<String, Listing> listings) throws Exception {
        long startTotal = System.currentTimeMillis();

        long startListings = System.currentTimeMillis();
        saveListings(listings);
        long endListings = System.currentTimeMillis();
        System.out.println("Tempo de inserção de listings: " + (endListings - startListings) + " ms");

        long startReviews = System.currentTimeMillis();
        saveReviews(listings);
        long endReviews = System.currentTimeMillis();
        System.out.println("Tempo de inserção de reviews: " + (endReviews - startReviews) + " ms");

        long endTotal = System.currentTimeMillis();
        System.out.println("Tempo total de inserção: " + (endTotal - startTotal) + " ms");
    }

    private void saveListings(Map<String, Listing> listings) throws Exception {
        String sql = "INSERT INTO listings " +
                "(id, name, host_id, host_name, neighbourhood_group, neighbourhood, latitude, longitude, " +
                "room_type, price, minimum_nights, number_of_reviews, last_review, reviews_per_month, " +
                "calculated_host_listings_count, availability_365, number_of_reviews_ltm, license) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int count = 0;
            for (Listing l : listings.values()) {
                stmt.setString(1, l.getId());
                stmt.setString(2, l.getName());
                stmt.setString(3, l.getHostId());
                stmt.setString(4, l.getHostName());
                stmt.setString(5, l.getNeighbourhoodGroup());
                stmt.setString(6, l.getNeighbourhood());
                stmt.setDouble(7, l.getLatitude());
                stmt.setDouble(8, l.getLongitude());
                stmt.setString(9, l.getRoomType());
                stmt.setInt(10, l.getPrice());
                stmt.setInt(11, l.getMinimumNights());
                stmt.setInt(12, l.getNumberOfReviews());

                if (l.getLastReview() != null && !l.getLastReview().isEmpty()) {
                    stmt.setDate(13, java.sql.Date.valueOf(l.getLastReview()));
                } else {
                    stmt.setNull(13, java.sql.Types.DATE);
                }

                stmt.setDouble(14, l.getReviewsPerMonth());
                stmt.setInt(15, l.getCalculatedHostListingsCount());
                stmt.setInt(16, l.getAvailability365());
                stmt.setInt(17, l.getNumberOfReviewsLtm());
                stmt.setString(18, l.getLicense());

                stmt.addBatch();
                count++;
                if (count % batchSize == 0) {
                    stmt.executeBatch();
                    System.out.println("Batch de listings inserido: " + count);
                }
            }
            stmt.executeBatch(); // executa o restante
            System.out.println("Último batch de listings inserido. Total: " + listings.size());
        }
    }

    private void saveReviews(Map<String, Listing> listings) throws Exception {
        String sql = "INSERT INTO reviews " +
                "(id, listing_id, date, reviewer_id, reviewer_name, comments) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            int count = 0;
            int totalReviews = 0;
            for (Listing l : listings.values()) {
                for (Review r : l.getReviews()) {
                    stmt.setString(1, r.getId());
                    stmt.setString(2, r.getListingId());

                    if (r.getDate() != null && !r.getDate().isEmpty()) {
                        stmt.setDate(3, java.sql.Date.valueOf(r.getDate()));
                    } else {
                        stmt.setNull(3, java.sql.Types.DATE);
                    }

                    stmt.setString(4, r.getReviewerId());
                    stmt.setString(5, r.getReviewerName());
                    stmt.setString(6, r.getComments());

                    stmt.addBatch();
                    count++;
                    totalReviews++;

                    if (count % batchSize == 0) {
                        stmt.executeBatch();
                        System.out.println("Batch de reviews inserido: " + totalReviews);
                    }
                }
            }
            stmt.executeBatch(); // executa o restante
            System.out.println("Último batch de reviews inserido. Total: " + totalReviews);
        }
    }
}

package org.example.util;



import com.opencsv.CSVReader;
import org.example.models.Listing;
import org.example.models.Review;

import java.io.FileReader;
import java.util.*;

public class CsvReader {

    // Ler listings do CSV e retornar Map<id, Listing>
    public static Map<String, Listing> loadListings(String listingsCsvPath) {
        Map<String, Listing> listings = new HashMap<>();

        try (CSVReader reader = new CSVReader(new FileReader(listingsCsvPath))) {
            String[] line;
            reader.readNext(); // pular cabeçalho

            while ((line = reader.readNext()) != null) {
                String id = line[0];
                String name = line[1];
                String hostId = line[2];
                String hostName = line[3];
                String neighbourhoodGroup = line[4];
                String neighbourhood = line[5];

                double latitude = line[6].isEmpty() ? 0.0 : Double.parseDouble(line[6]);
                double longitude = line[7].isEmpty() ? 0.0 : Double.parseDouble(line[7]);

                String roomType = line[8];
                int price = line[9].isEmpty() ? 0 : Integer.parseInt(line[9]);
                int minimumNights = line[10].isEmpty() ? 0 : Integer.parseInt(line[10]);
                int numberOfReviews = line[11].isEmpty() ? 0 : Integer.parseInt(line[11]);
                String lastReview = line[12].isEmpty() ? null : line[12];
                double reviewsPerMonth = line[13].isEmpty() ? 0.0 : Double.parseDouble(line[13]);
                int calculatedHostListingsCount = line[14].isEmpty() ? 0 : Integer.parseInt(line[14]);
                int availability365 = line[15].isEmpty() ? 0 : Integer.parseInt(line[15]);
                int numberOfReviewsLtm = line[16].isEmpty() ? 0 : Integer.parseInt(line[16]);
                String license = line[17];

                Listing listing = new Listing(id, name, hostId, hostName, neighbourhoodGroup, neighbourhood,
                        latitude, longitude, roomType, price, minimumNights, numberOfReviews,
                        lastReview, reviewsPerMonth, calculatedHostListingsCount,
                        availability365, numberOfReviewsLtm, license);

                listings.put(id, listing);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return listings;
    }

    // Ler reviews e associar às listings
    public static void loadReviews(String reviewsCsvPath, Map<String, Listing> listings) {
        try (CSVReader reader = new CSVReader(new FileReader(reviewsCsvPath))) {
            String[] line;
            reader.readNext(); // pular cabeçalho

            while ((line = reader.readNext()) != null) {
                String listingId = line[0];
                String id = line[1];
                String date = line[2];
                String reviewerId = line[3];
                String reviewerName = line[4];
                String comments = line[5];

                Review review = new Review(id, listingId, date, reviewerId, reviewerName, comments);

                Listing listing = listings.get(listingId);
                if (listing != null) {
                    listing.addReview(review);
                }
                // se não encontrar listing correspondente, ignora o review
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
// src/main/java/org/example/Repository/ListingRepositoryMongo.java
package org.example.Repository;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.example.models.Listing;
import org.example.models.Review;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ListingRepositoryMongo {

    private final MongoCollection<Document> collection;
    private final int batchSize = 500;

    public ListingRepositoryMongo(MongoDatabase database) {
        this.collection = database.getCollection("listings");
    }

    public void saveAll(Map<String, Listing> listings) {
        List<Document> batch = new ArrayList<>();
        int totalReviews = 0;

        for (Listing listing : listings.values()) {
            Document doc = new Document("id", listing.getId())
                    .append("name", listing.getName())
                    .append("hostId", listing.getHostId())
                    .append("hostName", listing.getHostName())
                    .append("neighbourhoodGroup", listing.getNeighbourhoodGroup())
                    .append("neighbourhood", listing.getNeighbourhood())
                    .append("latitude", listing.getLatitude())
                    .append("longitude", listing.getLongitude())
                    .append("roomType", listing.getRoomType())
                    .append("price", listing.getPrice())
                    .append("minimumNights", listing.getMinimumNights())
                    .append("numberOfReviews", listing.getNumberOfReviews())
                    .append("lastReview", listing.getLastReview())
                    .append("reviewsPerMonth", listing.getReviewsPerMonth())
                    .append("calculatedHostListingsCount", listing.getCalculatedHostListingsCount())
                    .append("availability365", listing.getAvailability365())
                    .append("numberOfReviewsLtm", listing.getNumberOfReviewsLtm())
                    .append("license", listing.getLicense());

            List<Document> reviewsDocs = new ArrayList<>();
            for (Review r : listing.getReviews()) {
                Document rDoc = new Document("id", r.getId())
                        .append("date", r.getDate())
                        .append("reviewerId", r.getReviewerId())
                        .append("reviewerName", r.getReviewerName())
                        .append("comments", r.getComments());
                reviewsDocs.add(rDoc);
                totalReviews++;
            }

            doc.append("reviews", reviewsDocs);
            batch.add(doc);

            if (batch.size() >= batchSize) {
                collection.insertMany(batch);
                batch.clear();
            }
        }

        if (!batch.isEmpty()) {
            collection.insertMany(batch);
        }

        System.out.println("Total de reviews inseridos: " + totalReviews);
    }
}
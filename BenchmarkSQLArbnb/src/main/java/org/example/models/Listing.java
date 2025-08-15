package org.example.models;

import java.util.ArrayList;
import java.util.List;

public class Listing {
    private String id;
    private String name;
    private String hostId;
    private String hostName;
    private String neighbourhoodGroup;
    private String neighbourhood;
    private double latitude;
    private double longitude;
    private String roomType;
    private int price;
    private int minimumNights;
    private int numberOfReviews;
    private String lastReview;
    private double reviewsPerMonth;
    private int calculatedHostListingsCount;
    private int availability365;
    private int numberOfReviewsLtm;
    private String license;

    private List<Review> reviews = new ArrayList<>();

    public Listing(String id, String name, String hostId, String hostName, String neighbourhoodGroup,
                   String neighbourhood, double latitude, double longitude, String roomType,
                   int price, int minimumNights, int numberOfReviews, String lastReview,
                   double reviewsPerMonth, int calculatedHostListingsCount, int availability365,
                   int numberOfReviewsLtm, String license) {
        this.id = id;
        this.name = name;
        this.hostId = hostId;
        this.hostName = hostName;
        this.neighbourhoodGroup = neighbourhoodGroup;
        this.neighbourhood = neighbourhood;
        this.latitude = latitude;
        this.longitude = longitude;
        this.roomType = roomType;
        this.price = price;
        this.minimumNights = minimumNights;
        this.numberOfReviews = numberOfReviews;
        this.lastReview = lastReview;
        this.reviewsPerMonth = reviewsPerMonth;
        this.calculatedHostListingsCount = calculatedHostListingsCount;
        this.availability365 = availability365;
        this.numberOfReviewsLtm = numberOfReviewsLtm;
        this.license = license;
    }

    // Getters e setters omitidos por brevidade

    public List<Review> getReviews() { return reviews; }
    public void addReview(Review review) { this.reviews.add(review); }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getHostId() {
        return hostId;
    }

    public void setHostId(String hostId) {
        this.hostId = hostId;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getNeighbourhoodGroup() {
        return neighbourhoodGroup;
    }

    public void setNeighbourhoodGroup(String neighbourhoodGroup) {
        this.neighbourhoodGroup = neighbourhoodGroup;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public String getNeighbourhood() {
        return neighbourhood;
    }

    public void setNeighbourhood(String neighbourhood) {
        this.neighbourhood = neighbourhood;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getRoomType() {
        return roomType;
    }

    public void setRoomType(String roomType) {
        this.roomType = roomType;
    }

    public int getMinimumNights() {
        return minimumNights;
    }

    public void setMinimumNights(int minimumNights) {
        this.minimumNights = minimumNights;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public int getNumberOfReviews() {
        return numberOfReviews;
    }

    public void setNumberOfReviews(int numberOfReviews) {
        this.numberOfReviews = numberOfReviews;
    }

    public String getLastReview() {
        return lastReview;
    }

    public void setLastReview(String lastReview) {
        this.lastReview = lastReview;
    }

    public double getReviewsPerMonth() {
        return reviewsPerMonth;
    }

    public void setReviewsPerMonth(double reviewsPerMonth) {
        this.reviewsPerMonth = reviewsPerMonth;
    }

    public int getCalculatedHostListingsCount() {
        return calculatedHostListingsCount;
    }

    public void setCalculatedHostListingsCount(int calculatedHostListingsCount) {
        this.calculatedHostListingsCount = calculatedHostListingsCount;
    }

    public int getAvailability365() {
        return availability365;
    }

    public void setAvailability365(int availability365) {
        this.availability365 = availability365;
    }

    public int getNumberOfReviewsLtm() {
        return numberOfReviewsLtm;
    }

    public void setNumberOfReviewsLtm(int numberOfReviewsLtm) {
        this.numberOfReviewsLtm = numberOfReviewsLtm;
    }

    public String getLicense() {
        return license;
    }

    public void setLicense(String license) {
        this.license = license;
    }

    public void setReviews(List<Review> reviews) {
        this.reviews = reviews;
    }
}

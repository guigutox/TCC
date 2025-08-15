package org.example.models;

public class Review {
    private String id;
    private String listingId;
    private String date;
    private String reviewerId;
    private String reviewerName;
    private String comments;

    public Review(String id, String listingId, String date, String reviewerId, String reviewerName, String comments) {
        this.id = id;
        this.listingId = listingId;
        this.date = date;
        this.reviewerId = reviewerId;
        this.reviewerName = reviewerName;
        this.comments = comments;
    }

    // Getters e setters
    public String getId() { return id; }
    public String getListingId() { return listingId; }
    public String getDate() { return date; }
    public String getReviewerId() { return reviewerId; }
    public String getReviewerName() { return reviewerName; }
    public String getComments() { return comments; }
}

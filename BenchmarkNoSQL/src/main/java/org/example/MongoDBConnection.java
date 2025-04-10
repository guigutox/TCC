package org.example;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class MongoDBConnection {
    private static final String CONNECTION_STRING = "mongodb://admin:password@localhost:27017";

    public static MongoDatabase getDatabase(String dbName) {
        MongoClient mongoClient = MongoClients.create(CONNECTION_STRING);
        return mongoClient.getDatabase(dbName);
    }
}

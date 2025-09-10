package org.example;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.github.cdimascio.dotenv.Dotenv;

public class MongoDBConnection {

    private static final Dotenv dotenv = Dotenv.load();

    public static MongoDatabase getDatabase() {
        String user = dotenv.get("DB_USER");
        String password = dotenv.get("DB_PASSWORD");
        String host = dotenv.get("DB_HOST");
        String port = dotenv.get("DB_PORT");
        String dbName = dotenv.get("DB_NAME");

        String connectionString = String.format(
                "mongodb://%s:%s@%s:%s", user, password, host, port
        );

        MongoClient mongoClient = MongoClients.create(connectionString);
        return mongoClient.getDatabase(dbName);
    }
}

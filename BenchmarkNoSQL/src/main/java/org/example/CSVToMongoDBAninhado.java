package org.example;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CSVToMongoDBAninhado {

    public static long importCSV(String animeFile, String userFile, String scoreFile, MongoCollection<Document> collection) {
        long startTime = System.currentTimeMillis();
        Map<String, Document> userDetailsMap = new HashMap<>();
        Map<String, Document> animeDetailsMap = new HashMap<>();
        Map<String, List<Document>> userScoresMap = new HashMap<>();

        try {
            // Carregar user_details
            loadUserDetails(userFile, userDetailsMap);

            // Carregar anime_details
            loadAnimeDetails(animeFile, animeDetailsMap);

            // Carregar e agrupar scores por usuário
            try (CSVParser parser = new CSVParser(new FileReader(scoreFile), CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
                int batchSize = 1000;
                for (CSVRecord record : parser) {
                    String userId = record.get("user_id");
                    String animeId = record.get("anime_id");

                    Document scoreDoc = new Document()
                            .append("anime_id", animeId)
                            .append("Anime Title", record.get("Anime Title"))
                            .append("rating", record.get("rating"))
                            .append("anime_details", animeDetailsMap.getOrDefault(animeId, new Document("info", "not found")));

                    userScoresMap.computeIfAbsent(userId, k -> new ArrayList<>()).add(scoreDoc);

                    // Quando atingir o limite de usuários únicos no mapa, insere os documentos
                    if (userScoresMap.size() >= batchSize) {
                        insertBatch(userScoresMap, userDetailsMap, collection);
                        userScoresMap.clear();
                    }
                }

                // Inserir registros restantes
                if (!userScoresMap.isEmpty()) {
                    insertBatch(userScoresMap, userDetailsMap, collection);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return System.currentTimeMillis() - startTime;
    }

    private static void loadUserDetails(String userFile, Map<String, Document> userDetailsMap) throws IOException {
        try (CSVParser parser = new CSVParser(new FileReader(userFile), CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                String userId = record.get("Mal ID");
                Document userDoc = new Document()
                        .append("Mal ID", userId)
                        .append("Username", record.get("Username"))
                        .append("Gender", record.get("Gender"))
                        .append("Birthday", record.get("Birthday"))
                        .append("Location", record.get("Location"))
                        .append("Joined", record.get("Joined"))
                        .append("Days Watched", record.get("Days Watched"))
                        .append("Mean Score", record.get("Mean Score"))
                        .append("Watching", record.get("Watching"))
                        .append("Completed", record.get("Completed"))
                        .append("On Hold", record.get("On Hold"))
                        .append("Dropped", record.get("Dropped"))
                        .append("Plan to Watch", record.get("Plan to Watch"))
                        .append("Total Entries", record.get("Total Entries"))
                        .append("Rewatched", record.get("Rewatched"))
                        .append("Episodes Watched", record.get("Episodes Watched"));
                userDetailsMap.put(userId, userDoc);
            }
        }
    }

    private static void loadAnimeDetails(String animeFile, Map<String, Document> animeDetailsMap) throws IOException {
        try (CSVParser parser = new CSVParser(new FileReader(animeFile), CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            for (CSVRecord record : parser) {
                String animeId = record.get("anime_id");
                Document animeDoc = new Document()
                        .append("anime_id", animeId)
                        .append("Name", record.get("Name"))
                        .append("English name", record.get("English name"))
                        .append("Other name", record.get("Other name"))
                        .append("Score", record.get("Score"))
                        .append("Genres", record.get("Genres"))
                        .append("Synopsis", record.get("Synopsis"))
                        .append("Type", record.get("Type"))
                        .append("Episodes", record.get("Episodes"))
                        .append("Aired", record.get("Aired"))
                        .append("Premiered", record.get("Premiered"))
                        .append("Status", record.get("Status"))
                        .append("Producers", record.get("Producers"))
                        .append("Licensors", record.get("Licensors"))
                        .append("Studios", record.get("Studios"))
                        .append("Source", record.get("Source"))
                        .append("Duration", record.get("Duration"))
                        .append("Rating", record.get("Rating"))
                        .append("Rank", record.get("Rank"))
                        .append("Popularity", record.get("Popularity"))
                        .append("Favorites", record.get("Favorites"))
                        .append("Scored By", record.get("Scored By"))
                        .append("Members", record.get("Members"))
                        .append("Image URL", record.get("Image URL"));
                animeDetailsMap.put(animeId, animeDoc);
            }
        }
    }

    private static void insertBatch(Map<String, List<Document>> userScoresMap, Map<String, Document> userDetailsMap, MongoCollection<Document> collection) {
        List<Document> batch = new ArrayList<>();
        for (Map.Entry<String, List<Document>> entry : userScoresMap.entrySet()) {
            String userId = entry.getKey();
            List<Document> scores = entry.getValue();

            Document original = userDetailsMap.getOrDefault(userId, new Document("info", "not found"));
            Document userDoc = new Document(original); // clona o documento original
            userDoc.append("scores", scores);
            batch.add(userDoc);
        }

        collection.insertMany(batch);
        System.out.println("Inserido batch com " + batch.size() + " documentos.");
    }
}

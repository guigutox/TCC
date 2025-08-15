package org.example;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CSVTOMongoDBSimples {

    public static long importCSV(String animeFile, String userFile, String scoreFile, MongoCollection<Document> collection) {
        long startTime = System.currentTimeMillis();
        Map<String, Document> userDetailsMap = new HashMap<>();
        Map<String, Document> animeDetailsMap = new HashMap<>();

        try {
            // Carregar detalhes dos usuários
            try (CSVParser parser = new CSVParser(new FileReader(userFile), CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
                for (CSVRecord record : parser) {
                    String userId = record.get("Mal ID");
                    Document userDoc = new Document();
                    for (String header : record.toMap().keySet()) {
                        userDoc.append(header, record.get(header));
                    }
                    userDetailsMap.put(userId, userDoc);
                }
            }

            // Carregar detalhes dos animes
            try (CSVParser parser = new CSVParser(new FileReader(animeFile), CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
                for (CSVRecord record : parser) {
                    String animeId = record.get("anime_id");
                    Document animeDoc = new Document();
                    for (String header : record.toMap().keySet()) {
                        animeDoc.append(header, record.get(header));
                    }
                    animeDetailsMap.put(animeId, animeDoc);
                }
            }

            // Para cada avaliação, criar um documento completo e inserir
            try (CSVParser parser = new CSVParser(new FileReader(scoreFile), CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
                int batchSize = 1000;
                int count = 0;
                java.util.List<Document> batch = new java.util.ArrayList<>();
                for (CSVRecord record : parser) {
                    String userId = record.get("user_id");
                    String animeId = record.get("anime_id");

                    Document doc = new Document();
                    // Dados do usuário
                    Document userDoc = userDetailsMap.get(userId);
                    if (userDoc != null) {
                        for (String key : userDoc.keySet()) {
                            doc.append("user_" + key, userDoc.get(key));
                        }
                    }
                    // Dados do anime
                    Document animeDoc = animeDetailsMap.get(animeId);
                    if (animeDoc != null) {
                        for (String key : animeDoc.keySet()) {
                            doc.append("anime_" + key, animeDoc.get(key));
                        }
                    }
                    // Dados da avaliação
                    for (String header : record.toMap().keySet()) {
                        doc.append(header, record.get(header));
                    }

                    batch.add(doc);
                    count++;
                    if (batch.size() >= batchSize) {
                        collection.insertMany(batch);
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    collection.insertMany(batch);
                }
                System.out.println("Total de avaliações inseridas: " + count);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return System.currentTimeMillis() - startTime;
    }
}
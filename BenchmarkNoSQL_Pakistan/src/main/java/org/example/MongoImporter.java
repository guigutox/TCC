package org.example;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class MongoImporter {

    private static final String DATABASE = "ecommerce";
    private static final String COLLECTION = "orders";
    private static final String FILE_PATH = "./src/main/resources/data/PakistanLargestEcommerceDataset.csv";
    private static final int BATCH_SIZE = 500;

    public void importData() {
        long startTime = System.currentTimeMillis();
        int insertedCount = 0;
        List<Document> batch = new ArrayList<>();

        MongoCredential credential = MongoCredential.createCredential("admin", "admin", "password".toCharArray());

        MongoClient mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(builder ->
                                builder.hosts(Collections.singletonList(new ServerAddress("localhost", 27017))))
                        .credential(credential)
                        .build()
        );

        MongoDatabase database = mongoClient.getDatabase(DATABASE);
        MongoCollection<Document> collection = database.getCollection(COLLECTION);

        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

        try (BufferedReader br = new BufferedReader(new FileReader(FILE_PATH))) {
            String headerLine = br.readLine(); // lê o cabeçalho
            if (headerLine == null) {
                System.err.println("Arquivo CSV está vazio.");
                return;
            }

            String[] headers = headerLine.split(";");
            String line;

            while ((line = br.readLine()) != null) {
                String[] values = line.split(";", -1);

                if (values.length != headers.length) {
                    System.out.println("❌ Linha ignorada (colunas diferentes): " + Arrays.toString(values));
                    continue;
                }

                Document doc = new Document();

                for (int i = 0; i < headers.length; i++) {
                    String key = headers[i].trim();
                    String value = values[i].trim();

                    if (value.equalsIgnoreCase("\\N") || value.isEmpty()) {
                        doc.append(key, null);
                        continue;
                    }

                    try {
                        switch (key) {
                            // Inteiros
                            case "item_id", "qty_ordered", "year", "month", "customer_id":
                                doc.append(key, Integer.parseInt(value));
                                break;
                            case "increment_id":
                                doc.append(key, Long.parseLong(value));
                                break;
                            // Decimais
                            case "price", "grand_total", "mv", "discount_amount":
                                doc.append(key, Double.parseDouble(value));
                                break;
                            // Datas
                            case "created_at", "working_date", "customer_since":
                                doc.append(key, LocalDate.parse(value, dateFormatter));
                                break;
                            // Strings
                            default:
                                doc.append(key, value);
                                break;
                        }
                    } catch (Exception e) {
                        doc.append(key, null); // evita erro se conversão falhar
                    }
                }

                batch.add(doc);

                if (batch.size() >= BATCH_SIZE) {
                    try {
                        collection.insertMany(batch);
                        insertedCount += batch.size();
                        System.out.println("✅ Batch de " + batch.size() + " documentos inserido com sucesso.");
                        batch.clear();
                    } catch (Exception e) {
                        System.out.println("Erro ao inserir documentos em batch: " + e.getMessage());
                    }
                }
            }

            if (!batch.isEmpty()) {
                try {
                    collection.insertMany(batch);
                    insertedCount += batch.size();
                    System.out.println("✅ Batch final de " + batch.size() + " documentos inserido com sucesso.");
                } catch (Exception e) {
                    System.out.println("Erro ao inserir documentos em batch final: " + e.getMessage());
                }
            }

        } catch (IOException e) {
            System.err.println("Erro ao ler o arquivo: " + e.getMessage());
        }

        long endTime = System.currentTimeMillis();
        System.out.println("✅ Total de documentos inseridos: " + insertedCount);
        System.out.println("⏱️ Tempo total de inserção: " + (endTime - startTime) + " ms");

        mongoClient.close();
    }
}

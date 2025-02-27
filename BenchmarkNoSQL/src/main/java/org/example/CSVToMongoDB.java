package org.example;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

public class CSVToMongoDB {

    private static final int BATCH_SIZE = 500;

    public static long importCSV(String csvFilePath, String collectionName, MongoDatabase database) {
        long startTime = System.currentTimeMillis();
        MongoCollection<Document> collection = database.getCollection(collectionName);

        try (Reader reader = new FileReader(csvFilePath);
             CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                     .withFirstRecordAsHeader() // Usa a primeira linha como cabeçalho
                     .withIgnoreSurroundingSpaces() // Ignora espaços ao redor dos valores
                     .withTrim())) { // Remove espaços em branco no início e no fim dos valores

            List<Document> documents = new ArrayList<>(BATCH_SIZE);
            int batchCount = 0;

            for (CSVRecord record : csvParser) {
                Document doc = new Document();

                // Itera sobre os cabeçalhos e adiciona os valores ao documento
                for (String header : csvParser.getHeaderNames()) {
                    String value = record.get(header);
                    doc.append(header, value);
                }

                documents.add(doc);

                // Insere o lote no MongoDB
                if (documents.size() >= BATCH_SIZE) {
                    collection.insertMany(documents);
                    batchCount++;
                    System.out.println("Lote " + batchCount + " inserido com sucesso! (" + documents.size() + " documentos)");
                    documents.clear();
                }
            }

            // Insere o último lote
            if (!documents.isEmpty()) {
                collection.insertMany(documents);
                batchCount++;
                System.out.println("Último lote " + batchCount + " inserido com sucesso! (" + documents.size() + " documentos)");
            }

            System.out.println("Arquivo " + csvFilePath + " importado com sucesso! Total de lotes: " + batchCount);
        } catch (IOException e) {
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        return endTime - startTime;
    }
}
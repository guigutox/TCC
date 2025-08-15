package org.example;

import com.mongodb.client.MongoCollection;
import org.bson.conversions.Bson;

import org.bson.Document;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BenchmarkUtils {

    // Adicione este método em BenchmarkUtils.java
// src/main/java/org/example/BenchmarkUtils.java
    public static void executarAggregacaoComTempo(MongoCollection<Document> collection, List<List<Document>> pipelines) {
        System.out.println("Executando benchmarks de agregação...");
        for (int i = 0; i < pipelines.size(); i++) {
            List<Document> pipeline = pipelines.get(i);
            System.out.println("Executando pipeline " + (i + 1) + ": " + pipeline);
            long start = System.currentTimeMillis();

            List<Document> resultado = collection.aggregate(pipeline).into(new ArrayList<>());

            long end = System.currentTimeMillis();
            System.out.println("Pipeline " + (i + 1) + " - Tempo de execução: " + (end - start) + " ms");
            System.out.println("Documentos encontrados: " + resultado.size());
        }
    }

    public static List<List<Document>> getAllPipelines() {
        List<List<Document>> pipelines = new ArrayList<>();

        List<Document> pipeline1 = new ArrayList<>();
        pipeline1.add(new Document("$unwind", "$scores"));
        pipeline1.add(new Document("$project", new Document("Username", 1)
                .append("rating", "$scores.rating")
                .append("_id", 0)));
        pipeline1.add(new Document("$limit", 10000)); // Ajuste conforme sua RAM


        List<Document> pipeline2 = new ArrayList<>();
        pipeline2.add(new Document("$unwind", "$scores"));
        pipeline2.add(new Document("$group", new Document("_id", new Document("Name", "$scores.Anime Title")
                .append("Score", "$scores.anime_details.Score"))));
        pipeline2.add(new Document("$project", new Document("_id", 0)
                .append("Name", "$_id.Name")
                .append("Score", "$_id.Score")));
        pipelines.add(pipeline2);

        return pipelines;
    }

}

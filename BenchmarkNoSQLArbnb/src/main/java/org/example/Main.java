package org.example;

import com.mongodb.client.MongoDatabase;
import org.example.models.Listing;
import org.example.util.CsvReader;
import org.example.Repository.ListingRepositoryMongo;
import org.example.repository.QueryExecutor;

import java.util.Map;
import java.util.Arrays;
import java.util.ArrayList;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;

public class Main {
    public static void main(String[] args) {

        String listingsCsv = "./src/main/resources/data/listings.csv";
        String reviewsCsv = "./src/main/resources/data/reviews.csv";

        // 1️⃣ Ler Listings
        Map<String, Listing> listings = CsvReader.loadListings(listingsCsv);

        // 2️⃣ Ler Reviews e associar aos Listings
        CsvReader.loadReviews(reviewsCsv, listings);

        // 3️⃣ Conectar ao MongoDB
        MongoDatabase database = MongoDBConnection.getDatabase();


        // 4️⃣ Inserir Listings no MongoDB
        ListingRepositoryMongo repo = new ListingRepositoryMongo(database);

        long startTime = System.currentTimeMillis();
        repo.saveAll(listings);
        long endTime = System.currentTimeMillis();

        System.out.println("Importação concluída. Total de Listings: " + listings.size());
        System.out.println("Tempo de inserção: " + (endTime - startTime) + " ms");

        runQueries(database);
    }



    private static void runQueries(MongoDatabase database) {
        QueryExecutor executor = new QueryExecutor(database);

        // 1️⃣ Listings com price > 200
        executor.addQuery(coll -> {
            var list = coll.find(gt("price", 200))
                    .into(new ArrayList<>());
            System.out.println("Query 1: Listings com preço maior que 200 -> " + list.size() + " documentos retornados");
        });

        // 2️⃣ Média de preço por bairro
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$neighbourhood", avg("avgPrice", "$price"))
            )).into(new ArrayList<>());
            System.out.println("Query 2: Média de preço por bairro -> " + list.size() + " documentos retornados");
        });

        // 3️⃣ Total de reviews por listing
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    project(new org.bson.Document("id", 1)
                            .append("numberOfReviews", new org.bson.Document("$size", "$reviews")))
            )).into(new ArrayList<>());
            System.out.println("Query 3: Total de reviews por listing -> " + list.size() + " documentos retornados");
        });

        // 4️⃣ Listings com disponibilidade total (availability_365 = 365)
        executor.addQuery(coll -> {
            var list = coll.find(eq("availability_365", 365))
                    .into(new ArrayList<>());
            System.out.println("Query 4: Listings disponíveis o ano inteiro (365 dias) -> " + list.size() + " documentos retornados");
        });

        // 5️⃣ Listings por tipo de quarto (room_type = 'Entire home/apt')
        executor.addQuery(coll -> {
            var list = coll.find(eq("room_type", "Entire home/apt"))
                    .into(new ArrayList<>());
            System.out.println("Query 5: Listings que são Entire home/apt -> " + list.size() + " documentos retornados");
        });

        // 6️⃣ Listings com mais de 50 reviews
        executor.addQuery(coll -> {
            var list = coll.find(gt("number_of_reviews", 50))
                    .into(new ArrayList<>());
            System.out.println("Query 6: Listings com mais de 50 reviews -> " + list.size() + " documentos retornados");
        });

        // 7️⃣ Média de reviews por bairro
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$neighbourhood", avg("avgReviews", "$number_of_reviews"))
            )).into(new ArrayList<>());
            System.out.println("Query 7: Média de reviews por bairro -> " + list.size() + " documentos retornados");
        });

        // Query 8: Total de reviews por bairro (usando unwind)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    // Desenrola os reviews
                    new org.bson.Document("$unwind", "$reviews"),
                    // Agrupa por bairro
                    group("$neighbourhood", avg("avgReviewsPerListing", "$reviews.reviewer_id"))
            )).into(new ArrayList<>());

            System.out.println("Query 8: Média de reviews por bairro (usando unwind) -> " + list.size() + " documentos retornados");
        });


        // Executar todas as queries
        executor.runAll();
    }

}

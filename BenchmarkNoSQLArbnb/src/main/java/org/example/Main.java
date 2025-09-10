package org.example;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BucketOptions;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.UnwindOptions;
import org.bson.Document;
import org.example.models.Listing;
import org.example.util.CsvReader;
import org.example.Repository.ListingRepositoryMongo;
import org.example.Repository.QueryExecutor;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.*;
import static com.mongodb.client.model.Projections.*;

import java.util.Map;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Scanner;

import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.descending;
import static com.mongodb.client.model.Projections.*;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;


public class Main {
    public static void main(String[] args) {
        int action = 0;

        if (args != null && args.length > 0) {
            try {
                action = Integer.parseInt(args[0].trim());
            } catch (NumberFormatException e) {
                System.out.println("Ação inválida. Use: 1 inserir | 2 buscar");
                return;
            }
        } else {
            System.out.print("Digite 1 para inserir ou 2 para buscar: ");
            Scanner sc = new Scanner(System.in);
            String input = sc.nextLine();
            try {
                action = Integer.parseInt(input.trim());
            } catch (NumberFormatException e) {
                System.out.println("Ação inválida. Use: 1 inserir | 2 buscar");
                return;
            }
        }

        switch (action) {
            case 1: {
                String listingsCsv = "./src/main/resources/data/listings.csv";
                String reviewsCsv = "./src/main/resources/data/reviews.csv";

                Map<String, Listing> listings = CsvReader.loadListings(listingsCsv);
                CsvReader.loadReviews(reviewsCsv, listings);

                MongoDatabase database = MongoDBConnection.getDatabase();
                ListingRepositoryMongo repo = new ListingRepositoryMongo(database);

                long startTime = System.currentTimeMillis();
                repo.saveAll(listings);
                long endTime = System.currentTimeMillis();

                System.out.println("Importação concluída. Total de Listings: " + listings.size());
                System.out.println("Tempo de inserção: " + (endTime - startTime) + " ms");
                break;
            }
            case 2: {
                MongoDatabase database = MongoDBConnection.getDatabase();
                runQueries(database);
                break;
            }
            default:
                System.out.println("Ação inválida. Use: 1 inserir | 2 buscar");
        }
    }

    private static void runQueries(MongoDatabase database) {
        QueryExecutor executor = new QueryExecutor(database);

       /* executor.addQuery(coll -> {
            var list = coll.find(gt("price", 200)).into(new ArrayList<>());
            System.out.println("Query 1: Listings com preço maior que 200 -> " + list.size() + " documentos retornados");
        });

        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$neighbourhood", avg("avgPrice", "$price"))
            )).into(new ArrayList<>());
            System.out.println("Query 2: Média de preço por bairro -> " + list.size() + " documentos retornados");
        });

        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    project(new org.bson.Document("id", 1)
                            .append("numberOfReviews", new org.bson.Document("$size", "$reviews")))
            )).into(new ArrayList<>());
            System.out.println("Query 3: Total de reviews por listing -> " + list.size() + " documentos retornados");
        });

        executor.addQuery(coll -> {
            var list = coll.find(eq("availability_365", 365)).into(new ArrayList<>());
            System.out.println("Query 4: Listings disponíveis o ano inteiro (365 dias) -> " + list.size() + " documentos retornados");
        });

        executor.addQuery(coll -> {
            var list = coll.find(eq("room_type", "Entire home/apt")).into(new ArrayList<>());
            System.out.println("Query 5: Listings que são Entire home/apt -> " + list.size() + " documentos retornados");
        });

        executor.addQuery(coll -> {
            var list = coll.find(gt("number_of_reviews", 50)).into(new ArrayList<>());
            System.out.println("Query 6: Listings com mais de 50 reviews -> " + list.size() + " documentos retornados");
        });

        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$neighbourhood", avg("avgReviews", "$number_of_reviews"))
            )).into(new ArrayList<>());
            System.out.println("Query 7: Média de reviews por bairro -> " + list.size() + " documentos retornados");
        });

        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    new org.bson.Document("$unwind", "$reviews"),
                    group("$neighbourhood", avg("avgReviewsPerListing", "$reviews.reviewer_id"))
            )).into(new ArrayList<>());
            System.out.println("Query 8: Média de reviews por bairro (usando unwind) -> " + list.size() + " documentos retornados");
        });*/

// 1) Listings por tipo de quarto
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$roomType", sum("total", 1)),
                    sort(descending("total"))
            )).into(new ArrayList<>());
            System.out.println("Q1: Listings por roomType -> " + list.size() + " grupos");
        });

// 2) Média de preço por bairro
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$neighbourhood", avg("avgPrice", "$price")),
                    sort(descending("avgPrice"))
            )).into(new ArrayList<>());
            System.out.println("Q2: Média de preço por neighbourhood -> " + list.size() + " bairros");
        });

// 3) Top 10 hosts com mais listings
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$hostId", sum("totalListings", 1)),
                    sort(descending("totalListings")),
                    limit(10)
            )).into(new ArrayList<>());
            System.out.println("Q3: Top 10 hosts por #listings -> " + list.size() + " hosts");
        });

// 4) Top 5 bairros mais caros (preço médio)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$neighbourhood", avg("avgPrice", "$price")),
                    sort(descending("avgPrice")),
                    limit(5)
            )).into(new ArrayList<>());
            System.out.println("Q4: Top 5 bairros mais caros -> " + list.size());
        });

// 5) Estatísticas de preço por roomType (min, max, avg)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$roomType",
                            min("minPrice", "$price"),
                            max("maxPrice", "$price"),
                            avg("avgPrice", "$price"))
            )).into(new ArrayList<>());
            System.out.println("Q5: Estatísticas de preço por roomType -> " + list.size());
        });

// 6) Contagem de listings com preço < 50
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    match(lt("price", 50)),
                    count("total")
            )).into(new ArrayList<>());
            System.out.println("Q6: Listings com price < 50 -> " + (list.isEmpty()?0:list.get(0).getInteger("total")));
        });

// 7) Hosts com mais de 50 listings
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$hostId", sum("totalListings", 1)),
                    match(gt("totalListings", 50)),
                    sort(descending("totalListings"))
            )).into(new ArrayList<>());
            System.out.println("Q7: Hosts com > 50 listings -> " + list.size());
        });

// 8) Distribuição de preços em faixas
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    new Document("$bucket", new Document("groupBy", "$price")
                            .append("boundaries", Arrays.asList(0, 50, 100, 200, 500, 1000))
                            .append("default", ">=1000")
                            .append("output", new Document("count", new Document("$sum", 1))))
            )).into(new ArrayList<>());
            System.out.println("Q8: Buckets de preço -> " + list.size());
        });

// Helpers para parse de datas string "YYYY-MM-DD[ ...]"
        var parseDateFromStr = (Document) new Document("$dateFromString",
                new Document("dateString", new Document("$substrCP", Arrays.asList("$$DATE_STR", 0, 10)))
                        .append("format", "%Y-%m-%d")
                        .append("onError", null));

// 9) Reviews por ano (reviews embutidos)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    unwind("$reviews"),
                    addFields(new Field<>("reviewDate",
                            new Document("$dateFromString",
                                    new Document("dateString", new Document("$substrCP", Arrays.asList("$reviews.date", 0, 10)))
                                            .append("format", "%Y-%m-%d")
                                            .append("onError", null)))),
                    project(new Document("year", new Document("$year", "$reviewDate"))),
                    group("$year", sum("totalReviews", 1)),
                    sort(ascending("_id"))
            )).into(new ArrayList<>());
            System.out.println("Q9: Reviews por ano -> " + list.size() + " anos");
        });

// 10) Top 10 listings por quantidade de reviews (usando tamanho do array)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    project(fields(include("id","name"),
                            computed("reviewsCount", new Document("$size", new Document("$ifNull", Arrays.asList("$reviews", new ArrayList<>())))))),
                    sort(descending("reviewsCount")),
                    limit(10)
            )).into(new ArrayList<>());
            System.out.println("Q10: Top 10 listings por #reviews -> " + list.size());
        });

// 11) Média do tamanho dos comentários por listing
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    unwind("$reviews", new UnwindOptions().preserveNullAndEmptyArrays(false)),
                    group("$id",
                            first("name", "$name"),
                            avg("avgCommentLen", new org.bson.Document("$strLenCP", "$reviews.comments"))),
                    sort(descending("avgCommentLen")),
                    limit(10)
            )).into(new ArrayList<>());
            System.out.println("Q11: Média do tamanho dos comentários por listing (top10) -> " + list.size());
        });

// 12) Top 10 reviewers mais ativos (por reviewerId)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    unwind("$reviews"),
                    group("$reviews.reviewerId",
                            first("reviewerName", "$reviews.reviewerName"),
                            sum("totalReviews", 1)),
                    sort(descending("totalReviews")),
                    limit(10)
            )).into(new ArrayList<>());
            System.out.println("Q12: Top 10 reviewers por #reviews -> " + list.size());
        });

// 13) Bairros com mais reviews (contagem de reviews embutidos)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    unwind("$reviews"),
                    group("$neighbourhood", sum("totalReviews", 1)),
                    sort(descending("totalReviews")),
                    limit(10)
            )).into(new ArrayList<>());
            System.out.println("Q13: Top bairros por #reviews -> " + list.size());
        });

// 14) Média de numberOfReviews (campo do listing) por bairro
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$neighbourhood", avg("avgNumberOfReviewsField", "$numberOfReviews")),
                    sort(descending("avgNumberOfReviewsField"))
            )).into(new ArrayList<>());
            System.out.println("Q14: Média do campo numberOfReviews por bairro -> " + list.size());
        });

// 15) Média de reviewsPerMonth por bairro
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$neighbourhood", avg("avgReviewsPerMonth", "$reviewsPerMonth")),
                    sort(descending("avgReviewsPerMonth"))
            )).into(new ArrayList<>());
            System.out.println("Q15: Média de reviewsPerMonth por bairro -> " + list.size());
        });

// 16) Disponibilidade média (availability365) por bairro
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$neighbourhood", avg("avgAvailability", "$availability365")),
                    sort(descending("avgAvailability"))
            )).into(new ArrayList<>());
            System.out.println("Q16: Disponibilidade média por bairro -> " + list.size());
        });

// 17) Total de reviews por host (somando reviews embutidos)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    unwind("$reviews"),
                    group("$hostId", sum("totalReviews", 1)),
                    sort(descending("totalReviews")),
                    limit(10)
            )).into(new ArrayList<>());
            System.out.println("Q17: Top hosts por #reviews -> " + list.size());
        });

// 18) Última data de review por listing
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    unwind("$reviews"),
                    addFields(new Field<>("reviewDate",
                            new Document("$dateFromString",
                                    new Document("dateString", new Document("$substrCP", Arrays.asList("$reviews.date", 0, 10)))
                                            .append("format", "%Y-%m-%d")
                                            .append("onError", null)))),
                    group("$id",
                            first("name", "$name"),
                            max("lastReviewDate", "$reviewDate")),
                    sort(descending("lastReviewDate")),
                    limit(10)
            )).into(new ArrayList<>());
            System.out.println("Q18: Última data de review por listing (top10 recentes) -> " + list.size());
        });

// 19) Listings com pelo menos 10 reviews
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    project(fields(include("id","name"),
                            computed("reviewsCount", new Document("$size", new Document("$ifNull", Arrays.asList("$reviews", new ArrayList<>())))))),
                    match(gte("reviewsCount", 10)),
                    sort(descending("reviewsCount")),
                    limit(20)
            )).into(new ArrayList<>());
            System.out.println("Q19: Listings com >=10 reviews -> " + list.size());
        });

// 20) Média de preço de listings com review no último ano
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    // Adiciona campo com quantidade de reviews
                    project(fields(
                            include("id", "name", "price", "roomType", "neighbourhood"),
                            computed("reviewsCount", new Document("$size", new Document("$ifNull", Arrays.asList("$reviews", new ArrayList<>()))))
                    )),
                    // Ordena primeiro por preço desc, depois por quantidade de reviews desc
                    sort(new Document("price", -1).append("reviewsCount", -1)),
                    limit(10)
            )).into(new ArrayList<>());

            System.out.println("Q20: Top 10 listings mais caros com mais reviews -> " + list.size());
            for (var doc : list) {
                System.out.println(doc.toJson());
            }
        });

        // 21) Média de preço por bairro + tipo de quarto
// Agrupa os listings por bairro e tipo de quarto, calcula o preço médio e ordena do mais caro para o mais barato
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group(new Document("neighbourhood", "$neighbourhood")
                                    .append("roomType", "$roomType"),
                            avg("avgPrice", "$price")),
                    sort(descending("avgPrice"))
            )).into(new ArrayList<>());
            System.out.println("Q21: Média de preço por bairro + roomType -> " + list.size() + " grupos");
            list.forEach(doc -> System.out.println(doc.toJson()));
        });

// 22) Top 5 bairros com mais reviews no último ano
// Considera apenas reviews do último ano, agrupa por bairro, conta o total de reviews e retorna os 5 maiores
        executor.addQuery(coll -> {
            var oneYearAgo = java.time.LocalDate.now().minusYears(1).toString(); // "YYYY-MM-DD"
            var list = coll.aggregate(Arrays.asList(
                    unwind("$reviews"),
                    match(gte("reviews.date", oneYearAgo)),
                    group("$neighbourhood", sum("totalReviews", 1)),
                    sort(descending("totalReviews")),
                    limit(5)
            )).into(new ArrayList<>());
            System.out.println("Q22: Top 5 bairros com mais reviews no último ano -> " + list.size());
            list.forEach(doc -> System.out.println(doc.toJson()));
        });

// 23) Hosts com maior preço médio entre seus listings
// Agrupa listings por host, calcula a média de preço e total de listings, filtra hosts com >=5 listings e retorna os 10 mais caros
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$hostId",
                            avg("avgPrice", "$price"),
                            sum("totalListings", 1)),
                    match(gte("totalListings", 5)),
                    sort(descending("avgPrice")),
                    limit(10)
            )).into(new ArrayList<>());
            System.out.println("Q23: Hosts com maior preço médio -> " + list.size());
            list.forEach(doc -> System.out.println(doc.toJson()));
        });

// 24) Ranking de listings por número de reviews
// Calcula a quantidade de reviews de cada listing, ordena do maior para o menor e retorna os 20 primeiros
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    project(fields(include("id","name"),
                            computed("reviewsCount", new Document("$size", new Document("$ifNull", Arrays.asList("$reviews", new ArrayList<>())))))),
                    sort(descending("reviewsCount")),
                    limit(20)
            )).into(new ArrayList<>());
            System.out.println("Q24: Ranking de listings por número de reviews -> " + list.size());
            list.forEach(doc -> System.out.println(doc.toJson()));
        });

// 25) Mediana de preço por bairro
// Agrupa os listings por bairro, cria array de preços, ordena o array e pega o valor do meio como mediana
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$neighbourhood",
                            push("prices", "$price")),
                    project(fields(include("_id"),
                            computed("medianPrice", new Document("$arrayElemAt", Arrays.asList(
                                    new Document("$sortArray", new Document("input", "$prices").append("sortBy", 1)),
                                    new Document("$floor", new Document("$divide", Arrays.asList(new Document("$size", "$prices"), 2)))
                            )))))
            )).into(new ArrayList<>());
            System.out.println("Q25: Mediana de preço por bairro -> " + list.size());
            list.forEach(doc -> System.out.println(doc.toJson()));
        });




        executor.runAll();
    }
}
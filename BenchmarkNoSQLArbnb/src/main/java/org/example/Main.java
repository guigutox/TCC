package org.example;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
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
            case 3:{
                MongoDatabase database = MongoDBConnection.getDatabase();
                limparBanco(database);
                break;
            }
            case 4:{
                MongoDatabase database = MongoDBConnection.getDatabase();
                aumentarPrecoListings(database);
                break;
            }
            case 5:{
                MongoDatabase database = MongoDBConnection.getDatabase();
                descontoQuartoPrivado(database);
                break;
            }
            case 6:{
                MongoDatabase database = MongoDBConnection.getDatabase();
                atualizarComentariosReviews(database);
                break;
            }
            default:
                System.out.println("Ação inválida. Use: 1 inserir | 2 buscar");
        }
    }

    public static void atualizarComentariosReviews(MongoDatabase database) {
        MongoCollection<Document> listings = database.getCollection("listings");
        // Atualiza reviews embutidos usando $[elem] (arrayFilters)
        Document filtro = new Document("reviews.date", new Document("$gte", "2023-01-01"));
        Document atualizacao = new Document("$set", new Document("reviews.$[elem].comments", "Atualizado para análise"));
        // Array filter para reviews com data >= 2023-01-01
        ArrayList<Document> arrayFilters = new ArrayList<>();
        arrayFilters.add(new Document("elem.date", new Document("$gte", "2023-01-01")));
        long start = System.currentTimeMillis();
        listings.updateMany(filtro, atualizacao, new com.mongodb.client.model.UpdateOptions().arrayFilters(arrayFilters));
        long end = System.currentTimeMillis();
        System.out.println("Comentários de reviews atualizados. Tempo: " + (end - start) + " ms.");
    }


    public static void descontoQuartoPrivado(MongoDatabase database) {
        MongoCollection<Document> listings = database.getCollection("listings");
        long start = System.currentTimeMillis();
        listings.updateMany(
                Filters.eq("roomType", "Private room"),
                Updates.mul("price", 0.9)
        );
        long end = System.currentTimeMillis();
        System.out.println("Desconto aplicado em quartos privados. Tempo: " + (end - start) + " ms.");
    }



    public static void aumentarPrecoListings(MongoDatabase database) {
        MongoCollection<Document> listings = database.getCollection("listings");
        Document filtro = new Document(); // Atualiza todos os documentos
        Document atualizacao = new Document("$inc", new Document("price", 10));
        long start = System.currentTimeMillis();
        listings.updateMany(filtro, atualizacao);
        long end = System.currentTimeMillis();
        System.out.println("Preço de todos os listings aumentado em 10. Tempo: " + (end - start) + " ms.");
    }



    public static void limparBanco(MongoDatabase database) {
        MongoCollection<Document> listings = database.getCollection("listings");
        long start = System.currentTimeMillis();
        listings.deleteMany(new Document());
        long end = System.currentTimeMillis();
        System.out.println("Banco limpo em " + (end - start) + " ms.");
    }



    private static void runQueries(MongoDatabase database) {
        QueryExecutor executor = new QueryExecutor(database);

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

// Q22: Top 5 room types com maior preço médio (listings com >=5 reviews)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    match(gte("numberOfReviews", 5)),          // Filtra listings com >=5 reviews
                    group("$roomType",
                            avg("avgPrice", "$price"),             // Calcula preço médio
                            sum("totalListings", 1)),              // Conta total de listings
                    sort(descending("avgPrice")),              // Ordena do mais caro para o mais barato
                    limit(5)                                   // Retorna top 5
            )).into(new ArrayList<>());

            System.out.println("Q22 nova: Top 5 room types com maior preço médio -> " + list.size());
            list.forEach(doc -> System.out.println(doc.toJson()));
        });


// 23) Hosts com maior preço médio entre seus listings
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
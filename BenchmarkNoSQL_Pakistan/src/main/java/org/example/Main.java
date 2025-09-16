package org.example;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UnwindOptions;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;
import org.example.MongoDBConnection;


import static com.mongodb.client.model.Accumulators.*;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.descending;
import static com.mongodb.client.model.Projections.*;




public class Main {
    public static void main(String[] args) {
        String filePath = "./src/main/resources/data/PakistanLargestEcommerceDataset.csv";
        Scanner scanner = new Scanner(System.in);
        MongoImporter importer = new MongoImporter();


        System.out.println("Selecione uma opção:");
        System.out.println("1 - Inserir dados no banco");
        System.out.println("2 - Executar buscas");

        int opcao = scanner.nextInt();

        switch (opcao) {
            case 1:
                importer.importData();
                System.out.println("Dados inseridos com sucesso!");
                break;
            case 2:
                MongoDatabase database = MongoDBConnection.getDatabase();
                runQueries(database);
                break;
            default:
                System.out.println("Opção inválida.");
        }


        /*long tempoBusca = importer.realizarBusca();
        System.out.println("Tempo de execução para busca: " + tempoBusca + " ms");*/
    }

    private static void runQueries(MongoDatabase database) {
        QueryExecutor executor = new QueryExecutor(database);

// Q1: Contagem de pedidos por status
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$status", sum("totalOrders", 1)),
                    sort(descending("totalOrders"))
            )).into(new ArrayList<>());
            System.out.println("Q1: Pedidos por status -> " + list.size());
        });

// Q2: Contagem de pedidos por método de pagamento
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$payment_method", sum("totalOrders", 1)),
                    sort(descending("totalOrders"))
            )).into(new ArrayList<>());
            System.out.println("Q2: Pedidos por payment_method -> " + list.size());
        });

// Q3: Pedidos por ano/mês (remove nulos)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    addFields(new Field<>("yearNum", new Document("$convert",
                            new Document("input", "$Year").append("to", "int").append("onError", null).append("onNull", null)))),
                    addFields(new Field<>("monthNum", new Document("$convert",
                            new Document("input", "$Month").append("to", "int").append("onError", null).append("onNull", null)))),
                    match(and(ne("yearNum", null), ne("monthNum", null))),
                    group(new Document("year", "$yearNum").append("month", "$monthNum"), sum("totalOrders", 1)),
                    sort(ascending("_id.year", "_id.month"))
            )).into(new ArrayList<>());
            System.out.println("Q3 ajustada -> " + list.size());
        });

// Q4: Top 10 clientes por faturamento (remove normalização extra)

        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    // 1) Normaliza possíveis nomes diferentes do campo customer para "cust"
                    new Document("$addFields", new Document("cust",
                            new Document("$ifNull", Arrays.asList(
                                    "$customer_id",
                                    "$Customer ID",
                                    "$customerId",
                                    "$cust_id",
                                    "$customerid",
                                    "$customer id"
                            )))),
                    // 2) Remove registros sem customer válido
                    new Document("$match", new Document("$and", Arrays.asList(
                            new Document("cust", new Document("$ne", null)),
                            new Document("cust", new Document("$ne", ""))
                    ))),
                    // 3) Agrupa por "cust" (já não será null) e soma grand_total convertido para double
                    new Document("$group", new Document("_id", "$cust")
                            .append("totalRevenue", new Document("$sum",
                                    new Document("$convert", new Document("input", "$grand_total")
                                            .append("to", "double").append("onError", 0).append("onNull", 0))))),
                    // 4) Ordena e limita
                    new Document("$sort", new Document("totalRevenue", -1)),
                    new Document("$limit", 10),
                    // 5) Renomeia para um output limpo (sem mostrar _id cru)
                    new Document("$project", new Document("_id", 0)
                            .append("customer", "$_id")
                            .append("totalRevenue", 1))
            )).into(new ArrayList<>());

            System.out.println("Q4_repl: Top 10 clientes por faturamento -> " + list.size());
            list.forEach(System.out::println);
        });


// Q5: Top 10 SKUs por quantidade vendida
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$sku",
                            sum("qty", new Document("$convert",
                                    new Document("input", "$qty_ordered").append("to", "int").append("onError", 0).append("onNull", 0)))),
                    sort(descending("qty")),
                    limit(10)
            )).into(new ArrayList<>());
            System.out.println("Q5: Top 10 SKUs por quantidade -> " + list.size());
        });

// Q6: Top 5 categorias por faturamento
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$category_name_1",
                            sum("revenue", new Document("$convert",
                                    new Document("input", "$grand_total").append("to", "double").append("onError", 0).append("onNull", 0)))),
                    sort(descending("revenue")),
                    limit(5)
            )).into(new ArrayList<>());
            System.out.println("Q6: Top 5 categorias por faturamento -> " + list.size());
        });

// Q7: Ticket médio (AVG grand_total) por status
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$status",
                            avg("avgTicket", new Document("$convert",
                                    new Document("input", "$grand_total").append("to", "double").append("onError", 0).append("onNull", 0))))
            )).into(new ArrayList<>());
            System.out.println("Q7: Ticket médio por status -> " + list.size());
        });

// Q8: Média de quantidade por SKU (top 10 por média)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$sku",
                            avg("avgQty", new Document("$convert",
                                    new Document("input", "$qty_ordered").append("to", "double").append("onError", 0).append("onNull", 0)))),
                    sort(descending("avgQty")),
                    limit(10)
            )).into(new ArrayList<>());
            System.out.println("Q8: Média de qty por SKU (top 10) -> " + list.size());
        });

// Q9: Total de pedidos e receita por payment_method
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$payment_method",
                            sum("totalOrders", 1),
                            sum("totalRevenue", "$grand_total")),
                    sort(descending("totalRevenue"))
            )).into(new ArrayList<>());
            System.out.println("Q9_repl: Total pedidos e receita por payment_method -> " + list.size());
            for (Object doc : list) {
                System.out.println(doc);
            }
        });


// Q10: Pedidos por dia da semana (usa números 0–6 como no SQL EXTRACT(DOW))
        executor.addQuery(coll -> {
            var dateExpr = new Document("$dateFromString",
                    new Document("dateString", "$Working Date").append("format", "%d/%m/%Y").append("onError", null).append("onNull", null));
            var list = coll.aggregate(Arrays.asList(
                    addFields(new Field<>("dow", new Document("$subtract", Arrays.asList(new Document("$dayOfWeek", dateExpr), 1)))),
                    match(ne("dow", null)),
                    group("$dow", sum("totalOrders", 1)),
                    sort(ascending("_id"))
            )).into(new ArrayList<>());
            System.out.println("Q10 ajustada -> " + list.size());
        });

// Q11: Faturamento total por ano
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    addFields(new Field<>("yearNum", new Document("$convert",
                            new Document("input", "$Year").append("to", "int").append("onError", null).append("onNull", null)))),
                    group("$yearNum",
                            sum("revenue", new Document("$convert",
                                    new Document("input", "$grand_total").append("to", "double").append("onError", 0).append("onNull", 0)))),
                    sort(ascending("_id"))
            )).into(new ArrayList<>());
            System.out.println("Q11: Faturamento por ano -> " + list.size());
        });

// Q12: Desconto total por categoria (mantém limit 10)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$category_name_1",
                            sum("totalDiscount", new Document("$convert",
                                    new Document("input", "$discount_amount").append("to", "double").append("onError", 0).append("onNull", 0)))),
                    sort(descending("totalDiscount")),
                    limit(10)
            )).into(new ArrayList<>());
            System.out.println("Q12 ajustada -> " + list.size());
        });

// Q13: Taxa de cancelamento por ano (canceled / total)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    addFields(new Field<>("yearNum", new Document("$convert",
                            new Document("input", "$Year").append("to", "int").append("onError", null).append("onNull", null)))),
                    group("$yearNum",
                            sum("total", 1),
                            sum("canceled", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$status", "canceled")), 1, 0)))),
                    project(new Document("year", "$_id").append("_id", 0)
                            .append("total", "$total")
                            .append("canceled", "$canceled")
                            .append("cancelRate", new Document("$cond", Arrays.asList(
                                    new Document("$gt", Arrays.asList("$total", 0)),
                                    new Document("$divide", Arrays.asList("$canceled", "$total")),
                                    0
                            )))),
                    sort(ascending("year"))
            )).into(new ArrayList<>());
            System.out.println("Q13: Taxa de cancelamento por ano -> " + list.size());
        });

// Q14: Receita COD vs não-COD por ano
        executor.addQuery(coll -> {
            var revenueExpr = new Document("$convert",
                    new Document("input", "$grand_total").append("to", "double").append("onError", 0).append("onNull", 0));
            var list = coll.aggregate(Arrays.asList(
                    addFields(new Field<>("yearNum", new Document("$convert",
                            new Document("input", "$Year").append("to", "int").append("onError", null).append("onNull", null)))),
                    group("$yearNum",
                            sum("codRevenue", new Document("$cond", Arrays.asList(new Document("$eq", Arrays.asList("$payment_method", "cod")), revenueExpr, 0))),
                            sum("nonCodRevenue", new Document("$cond", Arrays.asList(new Document("$ne", Arrays.asList("$payment_method", "cod")), revenueExpr, 0)))) ,
                    sort(ascending("_id"))
            )).into(new ArrayList<>());
            System.out.println("Q14: Receita COD vs não-COD por ano -> " + list.size());
            for (Object doc : list) {
                System.out.println(doc);
            }
        });

// Q15: Top 5 categorias por preço médio (campo price)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    group("$category_name_1",
                            avg("avgPrice", new Document("$convert",
                                    new Document("input", "$price").append("to", "double").append("onError", 0).append("onNull", 0)))),
                    sort(descending("avgPrice")),
                    limit(5)
            )).into(new ArrayList<>());
            System.out.println("Q15: Top 5 categorias por preço médio -> " + list.size());
            for (Object doc : list) {
                System.out.println(doc);
            }
        });

        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    // normaliza customer
                    new Document("$addFields", new Document("cust",
                            new Document("$ifNull", Arrays.asList(
                                    "$customer_id",
                                    "$Customer ID",
                                    "$customerId",
                                    "$cust_id",
                                    "$customerid",
                                    "$customer id"
                            )))),
                    // filtra cust válidos
                    new Document("$match", new Document("$and", Arrays.asList(
                            new Document("cust", new Document("$ne", null)),
                            new Document("cust", new Document("$ne", ""))
                    ))),
                    // agrupa por cliente: conta pedidos e soma receita convertendo grand_total
                    new Document("$group", new Document("_id", "$cust")
                            .append("orders", new Document("$sum", 1))
                            .append("revenue", new Document("$sum",
                                    new Document("$convert", new Document("input","$grand_total")
                                            .append("to","double").append("onError",0).append("onNull",0))))),
                    // ordena por orders e revenue (você pode trocar ordem)
                    new Document("$sort", new Document("orders", -1).append("revenue", -1)),
                    new Document("$limit", 5),
                    new Document("$project", new Document("_id", 0)
                            .append("customer", "$_id")
                            .append("orders", 1)
                            .append("revenue", 1))
            )).into(new ArrayList<>());

            System.out.println("Q16_repl: Top 5 clientes por pedidos e receita -> " + list.size());
            list.forEach(System.out::println);
        });

// Q17: Top 5 SKUs por faturamento em 2017
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    match(eq("Year", "2017")),
                    group("$sku",
                            sum("revenue", new Document("$convert",
                                    new Document("input", "$grand_total").append("to", "double").append("onError", 0).append("onNull", 0)))),
                    sort(descending("revenue")),
                    limit(5)
            )).into(new ArrayList<>());
            System.out.println("Q17: Top 5 SKUs por faturamento (2017) -> " + list.size());
        });

// Q18: Receita mensal de 2018 (ordenado por mês numérico)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    match(eq("Year", "2018")),
                    addFields(new Field<>("monthNum", new Document("$convert",
                            new Document("input", "$Month").append("to", "int").append("onError", null).append("onNull", null)))),
                    group("$monthNum",
                            sum("revenue", new Document("$convert",
                                    new Document("input", "$grand_total").append("to", "double").append("onError", 0).append("onNull", 0)))),
                    sort(ascending("_id"))
            )).into(new ArrayList<>());
            System.out.println("Q18: Receita mensal de 2018 -> " + list.size());
        });

// Q19: Taxa média de desconto por método de pagamento
        executor.addQuery(coll -> {
            var discount = new Document("$convert",
                    new Document("input", "$discount_amount").append("to", "double").append("onError", 0).append("onNull", 0));
            var grand = new Document("$convert",
                    new Document("input", "$grand_total").append("to", "double").append("onError", 0).append("onNull", 0));
            var denom = new Document("$add", Arrays.asList(discount, grand));
            var rate = new Document("$cond", Arrays.asList(
                    new Document("$gt", Arrays.asList(denom, 0)),
                    new Document("$divide", Arrays.asList(discount, denom)),
                    0
            ));
            var list = coll.aggregate(Arrays.asList(
                    addFields(new Field<>("discRate", rate)),
                    group("$payment_method", avg("avgDiscountRate", "$discRate")),
                    sort(descending("avgDiscountRate"))
            )).into(new ArrayList<>());
            System.out.println("Q19: Taxa média de desconto por método -> " + list.size());
        });

// Q20: Top 5 clientes por ticket médio (mínimo 2 pedidos)
        executor.addQuery(coll -> {
            var list = coll.aggregate(Arrays.asList(
                    // normaliza customer
                    new Document("$addFields", new Document("cust",
                            new Document("$ifNull", Arrays.asList(
                                    "$customer_id",
                                    "$Customer ID",
                                    "$customerId",
                                    "$cust_id",
                                    "$customerid",
                                    "$customer id"
                            )))),
                    // filtra cust válidos
                    new Document("$match", new Document("$and", Arrays.asList(
                            new Document("cust", new Document("$ne", null)),
                            new Document("cust", new Document("$ne", ""))
                    ))),
                    // agrupa e calcula avg e count (convertendo grand_total pra double)
                    new Document("$group", new Document("_id", "$cust")
                            .append("avgTicket", new Document("$avg",
                                    new Document("$convert", new Document("input","$grand_total")
                                            .append("to","double").append("onError",0).append("onNull",0))))
                            .append("totalOrders", new Document("$sum", 1))),
                    // exige pelo menos 2 pedidos
                    new Document("$match", new Document("totalOrders", new Document("$gte", 2))),
                    // ordena por ticket médio e limita
                    new Document("$sort", new Document("avgTicket", -1)),
                    new Document("$limit", 5),
                    new Document("$project", new Document("_id", 0)
                            .append("customer", "$_id")
                            .append("avgTicket", 1)
                            .append("totalOrders", 1))
            )).into(new ArrayList<>());

            System.out.println("Q20_repl: Top 5 clientes por ticket médio (>=2 pedidos) -> " + list.size());
            list.forEach(System.out::println);
        });



        executor.runAll();
    }
}

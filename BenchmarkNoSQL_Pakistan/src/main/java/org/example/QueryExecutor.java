package org.example;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class QueryExecutor {

    private final MongoCollection<Document> collection;

    public QueryExecutor(MongoDatabase database) {
        this.collection = database.getCollection("orders"); // aqui estava "listings"
    }


    // Interface funcional para uma query
    @FunctionalInterface
    public interface MongoQuery {
        void execute(MongoCollection<Document> collection);
    }

    private final List<MongoQuery> queries = new ArrayList<>();

    // Adicionar query à lista
    public void addQuery(MongoQuery query) {
        queries.add(query);
    }

    // Executar todas as queries e printar tempo
    public void runAll() {
        for (int i = 0; i < queries.size(); i++) {
            MongoQuery query = queries.get(i);
            long startTime = System.currentTimeMillis();
            query.execute(collection);
            long endTime = System.currentTimeMillis();
            System.out.println("Query " + (i+1) + " executada em " + (endTime - startTime) + " ms");
            System.out.println("-------------------------------------------------");
        }
    }
}

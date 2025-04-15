package org.example;

import com.mongodb.client.MongoCollection;
import org.bson.conversions.Bson;

import org.bson.Document;
import java.util.ArrayList;
import java.util.List;

public class BenchmarkUtils {

    public static List<Document> executarQueryComTempo(MongoCollection<Document> collection, Bson filtro) {
        long start = System.currentTimeMillis();

        List<Document> resultado = collection.find(filtro).into(new ArrayList<>());

        long end = System.currentTimeMillis();
        System.out.println("Tempo de execução: " + (end - start) + " ms");
        System.out.println("Documentos encontrados: " + resultado.size());

        return resultado;
    }

}

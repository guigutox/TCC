package org.example;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.example.CSVToMongoDBAninhado;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {

        Scanner ler = new Scanner(System.in);
        System.out.println("Definir metodo de insercao de dados: 1 - Normal, 2 - Aninhado");
        int op = ler.nextInt();

        if(op == 1){
            MongoDatabase database = MongoDBConnection.getDatabase("animeDB");

            // Caminhos dos arquivos CSV
            String animeDetailsCSV = "./src/main/resources/data/anime-dataset-2023.csv";
            String userDetailsCSV = "./src/main/resources/data/users-details-2023.csv";
            String userScoreCSV = "./src/main/resources/data/users-score-2023.csv";

            // Importação dos arquivos para o MongoDB
            long tempo = CSVToMongoDB.importCSV(animeDetailsCSV, "anime_details", database);
            System.out.println("Tempo de inserção dos animes  " + tempo + "ms");

            long tempoUser = CSVToMongoDB.importCSV(userDetailsCSV, "user_details", database);
            System.out.println("Tempo de inserção dos usuarios " + tempoUser + "ms");

            long tempoScore = CSVToMongoDB.importCSV(userScoreCSV, "user_score", database);
            System.out.println("Tempo de inserção dos scores " + tempoScore + "ms");
        }
        else if(op == 2){
            MongoDatabase database = MongoDBConnection.getDatabase("animeDB");
            MongoCollection<Document> collection = database.getCollection("anime_data");

            // Caminhos dos arquivos CSV
            String animeDetailsCSV = "./src/main/resources/data/anime-dataset-2023.csv";
            String userDetailsCSV = "./src/main/resources/data/users-details-2023.csv";
            String userScoreCSV = "./src/main/resources/data/users-score-2023.csv";

            // Importação dos arquivos para uma única coleção no MongoDB
            long tempo = CSVToMongoDBAninhado.importCSV(animeDetailsCSV, userDetailsCSV, userScoreCSV, collection);
            System.out.println("Tempo total de inserção: " + tempo + "ms");
        }
        else{
            System.out.println("Opcao invalida");
        }
    }
}



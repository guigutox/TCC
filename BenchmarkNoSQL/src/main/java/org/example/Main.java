package org.example;

import com.mongodb.client.MongoDatabase;

public class Main {
    public static void main(String[] args) {

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

}

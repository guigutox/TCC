package org.example;

public class Main {
    public static void main(String[] args) {
        String filePath = "./src/main/resources/data/PakistanLargestEcommerceDataset.csv";

        MongoImporter importer = new MongoImporter();
        importer.importData();

        /*long tempoBusca = importer.realizarBusca();
        System.out.println("Tempo de execução para busca: " + tempoBusca + " ms");*/
    }
}

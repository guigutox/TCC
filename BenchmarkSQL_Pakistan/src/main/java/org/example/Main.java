// src/main/java/org/example/Main.java
package org.example;

import java.sql.*;

public class Main {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/pakistan";
    private static final String DB_USER = "admin";
    private static final String DB_PASSWORD = "password";
    private static final String CSV_PATH = "./src/main/resources/data/PakistanLargestEcommerceDataset.csv";

    public static void main(String[] args) {
        PakistanImporter importer = new PakistanImporter();
        java.util.Scanner scanner = new java.util.Scanner(System.in);

        while (true) {
            System.out.println("\nMenu:");
            System.out.println("1 - Criar tabela");
            System.out.println("2 - Importar dados");
            System.out.println("3 - Limpar tabela");
            System.out.println("0 - Sair");
            System.out.print("Escolha uma opção: ");
            int opcao = scanner.nextInt();
            scanner.nextLine(); // consumir quebra de linha

            switch (opcao) {
                case 1:
                    importer.createTableIfNotExists();
                    break;
                case 2:
                    long tempo = importer.importData(CSV_PATH);
                    System.out.println("Tempo de importação: " + tempo + " ms");
                    break;
                case 3:
                    importer.clearTable();
                    break;
                case 0:
                    System.out.println("Saindo...");
                    return;
                default:
                    System.out.println("Opção inválida.");
            }
        }
    }
}
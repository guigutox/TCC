package org.example;

import java.io.FileReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import io.github.cdimascio.dotenv.Dotenv;


public class Main {

    private static Dotenv dotenv = Dotenv.load();
    private static final String DB_USER = dotenv.get("USER");
    private static final String DB_PASSWORD = dotenv.get("PASSWORD");
    private static final String DB_URL = dotenv.get("DB_URL");

    private static final String ANIMEDATASET_FILE_PATH = "./src/main/resources/data/anime-dataset-2023.csv"; // Caminho do CSV
    private static final String USUARIOSDATASET_FILE_PATH = "./src/main/resources/data/users-details-2023.csv";
    private static final String AVALIACOESDATASET_FILE_PATH = "./src/main/resources/data/users-score-2023.csv";
    //private static final String DB_URL = "jdbc:postgresql://localhost:5432/anime";
   // private static final String DB_USER = "postgres";
    //private static final String DB_PASSWORD = "postgress";

    public static void main(String[] args) {

        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();


        long memoryBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

    /*    long tempoAnimes = inserirAnimes();
        long tempoUsuarios = inserirUsarios();

        System.out.println("Tempo de execução para animes: " + tempoAnimes + "ms");
        System.out.println("Tempo de execução para usuarios: " + tempoUsuarios + "ms");
        System.out.println("Tempo de execução total das insercoes: " + (tempoAnimes + tempoUsuarios) + "ms");*/

        long tempoBusca = buscarAnime();
        System.out.println("Tempo de execução para buscar animes: " + buscarAnime() + "ms");


        long tempoAvaliacoes = inserirAvaliacoes();
        System.out.println("Tempo de execução para inserir avaliações: " + tempoAvaliacoes + "ms");

    }

    private static long inserirAvaliacoes() {
        long startTime = System.nanoTime();

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             CSVReader reader = new CSVReaderBuilder(new FileReader(AVALIACOESDATASET_FILE_PATH))
                     .withCSVParser(new CSVParserBuilder().withSeparator(',').build())
                     .build()) {

            connection.setAutoCommit(false);

            String checkUserQuery = "SELECT 1 FROM user_details WHERE Mal_ID = ?";
            String checkAnimeQuery = "SELECT 1 FROM anime WHERE anime_id = ?";
            String insertQuery = "INSERT INTO user_score (user_id, anime_id, rating) VALUES (?, ?, ?)";

            try (PreparedStatement checkUserStmt = connection.prepareStatement(checkUserQuery);
                 PreparedStatement checkAnimeQueryStmt = connection.prepareStatement(checkAnimeQuery);
                 PreparedStatement insertStmt = connection.prepareStatement(insertQuery)) {

                // 🔹 Pular cabeçalho
                reader.readNext();

                String[] nextLine;
                int lineNumber = 0;

                while ((nextLine = reader.readNext()) != null) {
                    try {
                        int userId = Integer.parseInt(nextLine[0]);
                        int animeId = Integer.parseInt(nextLine[2]);
                        int rating = Integer.parseInt(nextLine[4]);

                        // 🔹 Verifica se o usuário existe
                        checkUserStmt.setInt(1, userId);
                        try (ResultSet userResult = checkUserStmt.executeQuery()) {
                            if (!userResult.next()) {
                                System.out.println("Usuário não encontrado: " + userId);
                                continue;
                            }
                        }

                        // 🔹 Verifica se o anime existe
                        checkAnimeQueryStmt.setInt(1, animeId);
                        try (ResultSet animeResult = checkAnimeQueryStmt.executeQuery()) {
                            if (!animeResult.next()) {
                                System.out.println("Anime não encontrado: " + animeId);
                                continue;
                            }
                        }

                        // 🔹 Usa setStatementValue para configurar os parâmetros
                        setStatementValue(insertStmt, 1, userId, Types.INTEGER); // user_id
                        setStatementValue(insertStmt, 2, animeId, Types.INTEGER); // anime_id
                        setStatementValue(insertStmt, 3, rating, Types.INTEGER); // rating

                        // 🔹 Adiciona ao batch
                        insertStmt.addBatch();
                        lineNumber++;

                        // 🔹 A cada 500 inserções, executa o batch e faz commit
                        if (lineNumber % 500 == 0) {
                            insertStmt.executeBatch();
                            connection.commit();
                            System.out.println("Inseridos: " + lineNumber);
                        }

                    } catch (NumberFormatException e) {
                        System.out.println("Erro ao converter valores numéricos. Pulando linha: " + Arrays.toString(nextLine));
                    }
                }

                // 🔹 Executa o batch final e commita caso tenha sobrado dados
                insertStmt.executeBatch();
                connection.commit();
                System.out.println("✅ Inserção concluída. Total de linhas inseridas: " + lineNumber);

            } catch (SQLException e) {
                connection.rollback(); // Reverte a transação em caso de erro
                System.err.println("❌ Erro ao inserir no banco: " + e.getMessage());
                e.printStackTrace();
            }

        } catch (Exception e) {
            System.err.println("❌ Erro geral: " + e.getMessage());
            e.printStackTrace();
        }

        long endTime = System.nanoTime();
        return (endTime - startTime) / 1_000_000; // Retorna o tempo em milissegundos
    }

    // Método auxiliar para setar valores no PreparedStatement
    private static void setStatementValue(PreparedStatement stmt, int parameterIndex, Object value, int sqlType) throws SQLException {
        if (value == null) {
            stmt.setNull(parameterIndex, sqlType);
        } else {
            switch (sqlType) {
                case Types.INTEGER:
                    stmt.setInt(parameterIndex, (Integer) value);
                    break;
                case Types.VARCHAR:
                    stmt.setString(parameterIndex, (String) value);
                    break;
                case Types.DOUBLE:
                    stmt.setDouble(parameterIndex, (Double) value);
                    break;
                case Types.DATE:
                    stmt.setDate(parameterIndex, (java.sql.Date) value);
                    break;
                default:
                    throw new IllegalArgumentException("Tipo SQL não suportado: " + sqlType);
            }
        }
    }

    private static long inserirAnimes() {
        long startTime = System.nanoTime();
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             CSVReader reader = new CSVReaderBuilder(new FileReader(ANIMEDATASET_FILE_PATH))
                     .withCSVParser(new CSVParserBuilder().withSeparator(',').build())
                     .build()) {

            connection.setAutoCommit(false);

            String sql = "INSERT INTO anime (anime_id, name, english_name, other_name, score, genres, synopsis, type, episodes, aired, premiered, status, producers, licensors, studios, source, duration, rating, rank, popularity, favorites, scored_by, members, image_url) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                String[] row;
                int lineNumber = 0;
                reader.readNext(); // Pula o cabeçalho

                while ((row = reader.readNext()) != null) {
                    lineNumber++;

                    if (row.length != 24) {
                        System.err.println("⚠️ Linha " + lineNumber + " ignorada: " + Arrays.toString(row));
                        continue;
                    }

                    setStatementValue(stmt, 1, row[0], Types.INTEGER); // anime_id (INTEGER)
                    setStatementValue(stmt, 2, row[1], Types.VARCHAR); // name
                    setStatementValue(stmt, 3, row[2], Types.VARCHAR); // english_name
                    setStatementValue(stmt, 4, row[3], Types.VARCHAR); // other_name
                    setStatementValue(stmt, 5, row[4], Types.FLOAT);  // score (FLOAT)
                    setStatementValue(stmt, 6, row[5], Types.VARCHAR); // genres
                    setStatementValue(stmt, 7, row[6], Types.VARCHAR); // synopsis
                    setStatementValue(stmt, 8, row[7], Types.VARCHAR); // type
                    setStatementValue(stmt, 9, row[8], Types.INTEGER); // episodes (INTEGER)
                    setStatementValue(stmt, 10, row[9], Types.VARCHAR); // aired
                    setStatementValue(stmt, 11, row[10], Types.VARCHAR); // premiered
                    setStatementValue(stmt, 12, row[11], Types.VARCHAR); // status
                    setStatementValue(stmt, 13, row[12], Types.VARCHAR); // producers
                    setStatementValue(stmt, 14, row[13], Types.VARCHAR); // licensors
                    setStatementValue(stmt, 15, row[14], Types.VARCHAR); // studios
                    setStatementValue(stmt, 16, row[15], Types.VARCHAR); // source
                    setStatementValue(stmt, 17, row[16], Types.VARCHAR); // duration
                    setStatementValue(stmt, 18, row[17], Types.VARCHAR); // rating
                    setStatementValue(stmt, 19, row[18], Types.INTEGER); // rank (INTEGER)
                    setStatementValue(stmt, 20, row[19], Types.INTEGER); // popularity (INTEGER)
                    setStatementValue(stmt, 21, row[20], Types.INTEGER); // favorites (INTEGER)
                    setStatementValue(stmt, 22, row[21], Types.INTEGER); // scored_by (INTEGER)
                    setStatementValue(stmt, 23, row[22], Types.INTEGER); // members (INTEGER)
                    setStatementValue(stmt, 24, row[23], Types.VARCHAR); // image_url

                    //stmt.executeUpdate(); // Executa a inserção
                    stmt.addBatch();

                    if (lineNumber % 500 == 0) {
                        stmt.executeBatch();
                        connection.commit();
                    }
                }

                stmt.executeBatch();
                connection.commit();
                System.out.println("✅ Importação concluída com sucesso!");
                long endTime = System.nanoTime();
                return (endTime - startTime)/1000000;

            } catch (SQLException e) {
                connection.rollback();
                System.err.println("❌ Erro ao inserir no banco: " + e.getMessage());
            }

        } catch (Exception e) {
            System.err.println("❌ Erro geral: " + e.getMessage());
        }
        return 0;
    }

    // ✅ Método atualizado para converter corretamente números inteiros e decimais
    private static void setStatementValue(PreparedStatement stmt, int index, String value, int sqlType) throws SQLException {
        if (value == null || value.trim().isEmpty() || value.equalsIgnoreCase("UNKNOWN")) {
            stmt.setNull(index, sqlType);
        } else {
            switch (sqlType) {
                case Types.INTEGER:
                    stmt.setInt(index, (int) Double.parseDouble(value));
                    break;
                case Types.FLOAT:
                    stmt.setFloat(index, Float.parseFloat(value));
                    break;
                case Types.DATE:
                    // Se o valor for uma data em formato String, converte para java.sql.Date
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd"); // Formato esperado de data
                    try {
                        java.util.Date utilDate = sdf.parse(value); // Converte para java.util.Date
                        stmt.setDate(index, new java.sql.Date(utilDate.getTime())); // Converte para java.sql.Date
                    } catch (ParseException e) {
                        throw new SQLException("Erro ao converter a data: " + value, e);
                    }
                default:
                    stmt.setString(index, value);
            }
        }
    }

    private static long inserirUsarios() {
        long startTime = System.nanoTime();
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             CSVReader reader = new CSVReaderBuilder(new FileReader(USUARIOSDATASET_FILE_PATH))
                     .withCSVParser(new CSVParserBuilder().withSeparator(',').build())
                     .build()) {

            connection.setAutoCommit(false);

            String sql = "INSERT INTO user_details " +
                    "(Mal_ID, Username, Gender, Birthday, Location, Joined, Days_Watched, Mean_Score, Watching, Completed, On_Hold, Dropped, Plan_to_Watch, Total_Entries, Rewatched, Episodes_Watched) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                String[] row;
                int lineNumber = 0;
                reader.readNext(); // Pular o cabeçalho

                while ((row = reader.readNext()) != null) {
                    lineNumber++;

                    if (row.length != 16) {
                        System.out.println("⚠️ Linha " + lineNumber + " ignorada: " + Arrays.toString(row));
                        continue;
                    }

                    // Verificar dados antes de inserir
                    if (row[3].equals("0000-00-00")) {
                        row[3] = null;
                    }


                    setStatementValue(stmt, 1, row[0], Types.INTEGER); // Mal_ID
                    setStatementValue(stmt, 2, row[1], Types.VARCHAR); // Username
                    setStatementValue(stmt, 3, row[2], Types.VARCHAR); // Gender
                    setStatementValue(stmt, 4, row[3], Types.VARCHAR); // Birthday
                    setStatementValue(stmt, 5, row[4], Types.VARCHAR);   // Location
                    setStatementValue(stmt, 6, row[5], Types.VARCHAR); // Joined
                    setStatementValue(stmt, 7, row[6], Types.INTEGER);    // Days_Watched
                    setStatementValue(stmt, 8, row[7], Types.INTEGER); // Mean_Score
                    setStatementValue(stmt, 9, row[8], Types.INTEGER); // Watching
                    setStatementValue(stmt, 10, row[9], Types.INTEGER); // Completed
                    setStatementValue(stmt, 11, row[10], Types.INTEGER); // On_Hold
                    setStatementValue(stmt, 12, row[11], Types.INTEGER); // Dropped
                    setStatementValue(stmt, 13, row[12], Types.INTEGER); // Plan_to_Watch
                    setStatementValue(stmt, 14, row[13], Types.INTEGER); // Total_Entries
                    setStatementValue(stmt, 15, row[14], Types.INTEGER); // Rewatched
                    setStatementValue(stmt, 16, row[15], Types.INTEGER); // Episodes_Watched

                    stmt.addBatch(); // Adiciona ao batch

                    if (lineNumber % 500 == 0) {
                        stmt.executeBatch(); // Executa o batch
                        connection.commit(); // Confirma as alterações
                        System.out.println("Linha " + lineNumber + " inserida com sucesso!");
                    }
                }

                stmt.executeBatch(); // Executa o batch final
                connection.commit(); // Confirma as alterações
                System.out.println("✅ Importação concluída com sucesso!");

                Long endTime = System.nanoTime();
                return (endTime - startTime)/1000000;

            } catch (SQLException e) {
                connection.rollback(); // Reverte a transação em caso de erro
                System.err.println("❌ Erro ao inserir no banco: " + e.getMessage());
                e.printStackTrace(); // Detalhes do erro
            }

        } catch (Exception e) {
            System.err.println("❌ Erro geral: " + e.getMessage());
            e.printStackTrace(); // Detalhes do erro
        }
        return 0;
    }



    private static long buscarAnime() {

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement statement = connection.createStatement()) {
            long startTime = System.nanoTime();
            ResultSet rs = statement.executeQuery("EXPLAIN ANALYZE SELECT * FROM anime");
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000; // Converter para milissegundos


            try {
                while (rs != null && rs.next()) {
                    //System.out.println("Anime: " + rs.getString("name") + " | Score: " + rs.getFloat("score"));
                    System.out.println(rs.getString("QUERY PLAN"));
                }
                return duration;
            } catch (SQLException e) {
                System.err.println("❌ Erro ao processar o resultado: " + e.getMessage());
            }

        } catch (SQLException e) {
            System.err.println("❌ Erro ao executar a query: " + e.getMessage());

        }
        return 0;
    }



}







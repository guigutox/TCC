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
import java.util.Scanner;

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
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Escolha a operação:");
            System.out.println("[1] Inserção");
            System.out.println("[2] Consulta");
            System.out.println("[3] Limpar banco");
            System.out.println("[0] Sair");
            String opcao = scanner.nextLine().trim();

            switch (opcao) {
                case "1":
                    criarTabelas();
                    long tempoAnimes = inserirAnimes();
                    long tempoUsuarios = inserirUsarios();
                    long tempoAvaliacoes = inserirAvaliacoes();

                    System.out.println("Tempo de execução para animes: " + tempoAnimes + "ms");
                    System.out.println("Tempo de execução para usuarios: " + tempoUsuarios + "ms");
                    System.out.println("Tempo de execução para inserir avaliações: " + tempoAvaliacoes + "ms");
                    System.out.println("Tempo de execução total das insercoes: " + (tempoAnimes + tempoUsuarios + tempoAvaliacoes) + "ms");
                    break;
                case "2":
                    String[] queries = {
                            "EXPLAIN ANALYZE SELECT name FROM anime",
                            "EXPLAIN ANALYZE SELECT Username, rating FROM user_score us JOIN user_details ud ON us.user_id = ud.Mal_ID",
                            "EXPLAIN ANALYZE SELECT name, score FROM anime",
                            "EXPLAIN ANALYZE SELECT name, rank, popularity, favorites FROM anime",
                            "EXPLAIN ANALYZE SELECT ud.Username, us.anime_id FROM user_score us JOIN user_details ud ON us.user_id = ud.Mal_ID",
                            "EXPLAIN ANALYZE SELECT name, score FROM anime WHERE score > 9",
                            "EXPLAIN ANALYZE SELECT Username, rating FROM user_score us JOIN user_details ud ON us.user_id = ud.Mal_ID WHERE rating > 8",
                            "EXPLAIN ANALYZE SELECT name FROM anime WHERE type = 'Movie'",
                            "EXPLAIN ANALYZE SELECT name, studios FROM anime WHERE studios ILIKE '%Madhouse%'",
                            "EXPLAIN ANALYZE SELECT user_id, anime_id, rating FROM user_score WHERE anime_id > 1000",
                            "EXPLAIN ANALYZE SELECT ud.Username, a.name FROM user_score us JOIN user_details ud ON us.user_id = ud.Mal_ID JOIN anime a ON us.anime_id = a.anime_id",
                            "EXPLAIN ANALYZE SELECT a.name, us.rating FROM user_score us JOIN anime a ON us.anime_id = a.anime_id",
                            "EXPLAIN ANALYZE SELECT ud.Username, a.name, a.score FROM user_score us JOIN user_details ud ON us.user_id = ud.Mal_ID JOIN anime a ON us.anime_id = a.anime_id",
                            "EXPLAIN ANALYZE SELECT DISTINCT ud.Username FROM user_score us JOIN user_details ud ON us.user_id = ud.Mal_ID JOIN anime a ON us.anime_id = a.anime_id WHERE a.genres ILIKE '%Action%'",
                            "EXPLAIN ANALYZE SELECT a.name, ud.Username FROM user_score us JOIN anime a ON us.anime_id = a.anime_id JOIN user_details ud ON us.user_id = ud.Mal_ID",
                            "EXPLAIN ANALYZE SELECT a.name, ud.Username FROM user_score us JOIN anime a ON us.anime_id = a.anime_id JOIN user_details ud ON us.user_id = ud.Mal_ID WHERE a.members > 100000",
                            "EXPLAIN ANALYZE SELECT ud.Username, a.name, us.rating FROM user_score us JOIN user_details ud ON us.user_id = ud.Mal_ID JOIN anime a ON us.anime_id = a.anime_id WHERE us.rating > 9 AND a.score < 7",
                            "EXPLAIN ANALYZE SELECT a.name FROM user_score us JOIN anime a ON us.anime_id = a.anime_id WHERE us.rating = 10 AND a.licensors ILIKE '%Funimation%'",
                            "EXPLAIN ANALYZE SELECT COUNT(*) AS total_romance_baixa_nota FROM user_score us JOIN anime a ON us.anime_id = a.anime_id WHERE a.genres ILIKE '%Romance%' AND us.rating < 6",
                            "EXPLAIN ANALYZE SELECT DISTINCT ud.Username FROM user_score us JOIN anime a ON us.anime_id = a.anime_id JOIN user_details ud ON us.user_id = ud.Mal_ID WHERE a.type = 'OVA' AND us.rating > 7",
                            "EXPLAIN ANALYZE SELECT name, genres FROM anime",
                            "EXPLAIN ANALYZE SELECT username FROM user_details",
                            "EXPLAIN ANALYZE SELECT anime_title, rating FROM user_score JOIN anime ON user_score.anime_id = anime.anime_id",
                            "EXPLAIN ANALYZE SELECT * FROM user_details WHERE gender = 'Male'",
                            "EXPLAIN ANALYZE SELECT name, image_url FROM anime WHERE image_url LIKE 'https%'",
                            "EXPLAIN ANALYZE SELECT anime_title FROM user_score WHERE user_id = 68110",
                            "EXPLAIN ANALYZE SELECT anime_title FROM user_score WHERE user_id = 68110 AND rating > 3",
                            "EXPLAIN ANALYZE SELECT a.name FROM user_score us JOIN anime a ON us.anime_id = a.anime_id WHERE us.user_id = 68110 AND us.rating > 3",
                            "EXPLAIN ANALYZE SELECT * FROM user_details WHERE TO_DATE(joined, 'YYYY-MM-DD\"T\"HH24:MI:SS\"+00:00\"') > '2015-01-01'",
                            "EXPLAIN ANALYZE SELECT name, SUBSTRING(image_url FROM 'https?://([^/]+)') AS domain FROM anime",
                            "EXPLAIN ANALYZE SELECT u.username, a.name, s.rating FROM user_score s JOIN user_details u ON s.user_id = u.Mal_ID JOIN anime a ON s.anime_id = a.anime_id",
                            "EXPLAIN ANALYZE SELECT DISTINCT a.name FROM user_details u JOIN user_score s ON u.Mal_ID = s.user_id JOIN anime a ON s.anime_id = a.anime_id WHERE u.gender = 'Female'",
                            "EXPLAIN ANALYZE SELECT a.name, AVG(s.rating) AS media_nota FROM anime a JOIN user_score s ON a.anime_id = s.anime_id GROUP BY a.name",
                            "EXPLAIN ANALYZE SELECT u.username, AVG(s.rating) AS media FROM user_details u JOIN user_score s ON u.Mal_ID = s.user_id GROUP BY u.username",
                            "EXPLAIN ANALYZE SELECT u.username, COUNT(s.anime_id) AS total_animes, AVG(s.rating) AS media_score FROM user_details u JOIN user_score s ON u.Mal_ID = s.user_id GROUP BY u.username HAVING COUNT(s.anime_id) > 10",
                            "EXPLAIN ANALYZE SELECT a.name, AVG(s.rating) AS media_score FROM user_details u JOIN user_score s ON u.Mal_ID = s.user_id JOIN anime a ON s.anime_id = a.anime_id WHERE u.gender = 'Male' AND a.genres ILIKE '%Action%' GROUP BY a.name",
                            "EXPLAIN ANALYZE SELECT EXTRACT(YEAR FROM AGE(TO_DATE(birthday, 'YYYY-MM-DD'))) AS idade, AVG(s.rating) AS media_score FROM user_details u JOIN user_score s ON u.Mal_ID = s.user_id WHERE birthday IS NOT NULL GROUP BY idade ORDER BY idade",
                            "EXPLAIN ANALYZE SELECT a.name, a.popularity, AVG(s.rating) AS media FROM anime a JOIN user_score s ON a.anime_id = s.anime_id GROUP BY a.anime_id ORDER BY a.popularity ASC, media DESC LIMIT 5",
                            "EXPLAIN ANALYZE SELECT gender, COUNT(*) AS qtd_usuarios, AVG(s.rating) AS media_score FROM user_details u JOIN user_score s ON u.Mal_ID = s.user_id WHERE gender IS NOT NULL GROUP BY gender",
                            "EXPLAIN ANALYZE SELECT a.name, AVG(s.rating) AS media FROM anime a JOIN user_score s ON a.anime_id = s.anime_id WHERE a.aired ~ '([0-9]{4})' AND CAST(SUBSTRING(a.aired FROM '([0-9]{4})') AS INT) < 2010 GROUP BY a.name HAVING AVG(s.rating) > 8",
                            "EXPLAIN ANALYZE SELECT AVG(EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM TO_DATE(Birthday, 'YYYY-MM-DD'))) AS media_idade FROM user_details ud JOIN user_score us ON ud.Mal_ID = us.user_id WHERE us.anime_id = 1",
                            "EXPLAIN ANALYZE SELECT Gender, COUNT(*) AS total FROM user_details GROUP BY Gender",
                            "EXPLAIN ANALYZE SELECT * FROM user_details WHERE TO_DATE(Joined, 'YYYY-MM-DD') > '2020-01-01'",
                            "EXPLAIN ANALYZE SELECT SUBSTRING(image_url FROM 'https://[^ ]*') AS url_limpa FROM anime",
                            "EXPLAIN ANALYZE SELECT a.* FROM anime a JOIN user_score us ON a.anime_id = us.anime_id JOIN user_details ud ON ud.Mal_ID = us.user_id WHERE ud.Username = 'Guilherme'",
                            "EXPLAIN ANALYZE SELECT a.* FROM anime a JOIN user_score us ON a.anime_id = us.anime_id JOIN user_details ud ON ud.Mal_ID = us.user_id WHERE ud.Username = 'Guilherme' AND us.rating > 3",
                            "EXPLAIN ANALYZE SELECT a.name FROM anime a JOIN user_score us ON a.anime_id = us.anime_id JOIN user_details ud ON ud.Mal_ID = us.user_id WHERE ud.Username = 'Guilherme' AND us.rating > 3",
                            "EXPLAIN ANALYZE SELECT a.name, us.rating, ud.Username FROM anime a JOIN user_score us ON a.anime_id = us.anime_id JOIN user_details ud ON us.user_id = ud.Mal_ID",
                            "EXPLAIN ANALYZE SELECT AVG(us.rating) AS media_rating FROM user_details ud JOIN user_score us ON ud.Mal_ID = us.user_id JOIN anime a ON a.anime_id = us.anime_id WHERE EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM TO_DATE(ud.Birthday, 'YYYY-MM-DD')) = 25 AND a.genres ILIKE '%Action%'"
                    };
                    executarQueriesComTempo(queries);
                    break;
                case "3":
                    limparBanco();
                    break;
                case "0":
                    System.out.println("Encerrando...");
                    scanner.close();
                    return;
                default:
                    System.out.println("Opção inválida. Escolha 1, 2, 3 ou 0.");
            }
        }
    }

    private static void criarTabelas() {
        System.out.println("⏳ Verificando e criando tabelas...");

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement statement = connection.createStatement()) {

            // Desativar verificações de FK temporariamente para evitar erros durante a criação
            statement.execute("SET CONSTRAINTS ALL DEFERRED");

            // Tabela anime
            String createAnimeTable = "CREATE TABLE IF NOT EXISTS anime (" +
                    "anime_id INT PRIMARY KEY, " +
                    "name VARCHAR(255), " +
                    "english_name VARCHAR(255), " +
                    "other_name VARCHAR(255), " +
                    "score FLOAT, " +
                    "genres TEXT, " +
                    "synopsis TEXT, " +
                    "type VARCHAR(50), " +
                    "episodes INT, " +
                    "aired VARCHAR(100), " +
                    "premiered VARCHAR(50), " +
                    "status VARCHAR(50), " +
                    "producers TEXT, " +
                    "licensors TEXT, " +
                    "studios TEXT, " +
                    "source VARCHAR(100), " +
                    "duration VARCHAR(50), " +
                    "rating VARCHAR(50), " +
                    "rank INT, " +
                    "popularity INT, " +
                    "favorites INT, " +
                    "scored_by INT, " +
                    "members INT, " +
                    "image_url TEXT)";
            statement.execute(createAnimeTable);

            // Tabela user_details
            String createUserDetailsTable = "CREATE TABLE IF NOT EXISTS user_details (" +
                    "Mal_ID INT PRIMARY KEY, " +
                    "Username VARCHAR(255), " +
                    "Gender VARCHAR(10), " +
                    "Birthday VARCHAR(50), " +
                    "Location VARCHAR(255), " +
                    "Joined VARCHAR(50), " +
                    "Days_Watched INT, " +
                    "Mean_Score INT, " +
                    "Watching INT, " +
                    "Completed INT, " +
                    "On_Hold INT, " +
                    "Dropped INT, " +
                    "Plan_to_Watch INT, " +
                    "Total_Entries INT, " +
                    "Rewatched INT, " +
                    "Episodes_Watched INT)";
            statement.execute(createUserDetailsTable);

            // Tabela user_score
            String createUserScoreTable = "CREATE TABLE IF NOT EXISTS user_score (" +
                    "user_id INT, " +
                    "anime_id INT, " +
                    "rating INT, " +
                    "PRIMARY KEY (user_id, anime_id), " +
                    "FOREIGN KEY (user_id) REFERENCES user_details(Mal_ID), " +
                    "FOREIGN KEY (anime_id) REFERENCES anime(anime_id))";
            statement.execute(createUserScoreTable);

            System.out.println("✅ Tabelas verificadas/criadas com sucesso!");

        } catch (SQLException e) {
            System.err.println("❌ Erro ao criar tabelas: " + e.getMessage());
            e.printStackTrace();
        }
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



    private static long realizarQuery(String query) {

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement statement = connection.createStatement()) {
            long startTime = System.nanoTime();
            ResultSet rs = statement.executeQuery(query);
            long endTime = System.nanoTime();
            long duration = (endTime - startTime) / 1000000;

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

    private static void executarQueriesComTempo(String[] queries) {
        int totalQueriesExecutadas = 0;
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement statement = connection.createStatement()) {
            for (int i = 0; i < queries.length; i++) {
                String query = queries[i];
                System.out.println((i + 1) + "º query:");
                long startTime = System.nanoTime();
                ResultSet rs = statement.executeQuery(query);
                long endTime = System.nanoTime();
                long duration = (endTime - startTime) / 1_000_000;
                System.out.println("Tempo de execução: " + duration + "ms");

                while (rs != null && rs.next()) {
                    System.out.println(rs.getString("QUERY PLAN"));
                }
                totalQueriesExecutadas++;
            }
            System.out.println("Total de queries executadas: " + totalQueriesExecutadas);
        } catch (SQLException e) {
            System.err.println("Erro ao executar queries: " + e.getMessage());
        }
    }

    private static void limparBanco() {
        System.out.println("⏳ Limpando todas as tabelas...");
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement statement = connection.createStatement()) {

            // Desativa restrições de FK
            statement.execute("SET CONSTRAINTS ALL DEFERRED");

            // Limpa as tabelas
            statement.execute("TRUNCATE TABLE user_score, user_details, anime RESTART IDENTITY CASCADE");

            System.out.println("✅ Banco limpo com sucesso!");
        } catch (SQLException e) {
            System.err.println("❌ Erro ao limpar banco: " + e.getMessage());
        }
    }





}







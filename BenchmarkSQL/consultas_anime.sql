
-- ============================
-- 1. Projeções simples
-- ============================

-- 1. Listar os nomes dos animes
SELECT name FROM anime;

-- 2. Listar o nome de usuário e a nota atribuída
SELECT Username, rating FROM user_score us
JOIN user_details ud ON us.user_id = ud.Mal_ID;

-- 3. Listar o nome do anime e seu score
SELECT name, score FROM anime;

-- 4. Listar rank, popularity e favorites dos animes
SELECT name, rank, popularity, favorites FROM anime;

-- 5. Listar usuários e IDs dos animes que eles avaliaram
SELECT ud.Username, us.anime_id
FROM user_score us
JOIN user_details ud ON us.user_id = ud.Mal_ID;

-- ============================
-- 2. Filtros com WHERE
-- ============================

-- 6. Animes com score maior que 9
SELECT name, score FROM anime WHERE score > 9;

-- 7. Usuários que deram nota maior que 8
SELECT Username, rating
FROM user_score us
JOIN user_details ud ON us.user_id = ud.Mal_ID
WHERE rating > 8;

-- 8. Animes do tipo "Movie"
SELECT name FROM anime WHERE type = 'Movie';

-- 9. Animes do estúdio “Madhouse”
SELECT name, studios FROM anime WHERE studios ILIKE '%Madhouse%';

-- 10. Avaliações feitas para animes com ID maior que 1000
SELECT user_id, anime_id, rating FROM user_score WHERE anime_id > 1000;

-- ============================
-- 3. Junções entre tabelas
-- ============================

-- 11. Nome dos usuários e os nomes dos animes que eles avaliaram
SELECT ud.Username, a.name
FROM user_score us
JOIN user_details ud ON us.user_id = ud.Mal_ID
JOIN anime a ON us.anime_id = a.anime_id;

-- 12. Nome dos animes e suas notas atribuídas
SELECT a.name, us.rating
FROM user_score us
JOIN anime a ON us.anime_id = a.anime_id;

-- 13. Nome dos usuários e o score geral do anime que avaliaram
SELECT ud.Username, a.name, a.score
FROM user_score us
JOIN user_details ud ON us.user_id = ud.Mal_ID
JOIN anime a ON us.anime_id = a.anime_id;

-- 14. Usuários que avaliaram animes do gênero “Action”
SELECT DISTINCT ud.Username
FROM user_score us
JOIN user_details ud ON us.user_id = ud.Mal_ID
JOIN anime a ON us.anime_id = a.anime_id
WHERE a.genres ILIKE '%Action%';

-- 15. Nome do anime e os usuários que o avaliaram
SELECT a.name, ud.Username
FROM user_score us
JOIN anime a ON us.anime_id = a.anime_id
JOIN user_details ud ON us.user_id = ud.Mal_ID;

-- ============================
-- 4. Junções com filtros
-- ============================

-- 16. Animes com mais de 100.000 membros avaliados
SELECT a.name, ud.Username
FROM user_score us
JOIN anime a ON us.anime_id = a.anime_id
JOIN user_details ud ON us.user_id = ud.Mal_ID
WHERE a.members > 100000;

-- 17. Usuários que deram nota > 9 para animes com score < 7
SELECT ud.Username, a.name, us.rating
FROM user_score us
JOIN user_details ud ON us.user_id = ud.Mal_ID
JOIN anime a ON us.anime_id = a.anime_id
WHERE us.rating > 9 AND a.score < 7;

-- 18. Nome dos animes avaliados com nota 10 e licenciados por “Funimation”
SELECT a.name
FROM user_score us
JOIN anime a ON us.anime_id = a.anime_id
WHERE us.rating = 10 AND a.licensors ILIKE '%Funimation%';

-- 19. Quantidade de animes do gênero "Romance" avaliados com nota menor que 6
SELECT COUNT(*) AS total_romance_baixa_nota
FROM user_score us
JOIN anime a ON us.anime_id = a.anime_id
WHERE a.genres ILIKE '%Romance%' AND us.rating < 6;

-- 20. Usuários que avaliaram animes do tipo “OVA” com nota maior que 7
SELECT DISTINCT ud.Username
FROM user_score us
JOIN anime a ON us.anime_id = a.anime_id
JOIN user_details ud ON us.user_id = ud.Mal_ID
WHERE a.type = 'OVA' AND us.rating > 7;


-- -------------------------
-- CONSULTAS: PROJEÇÕES (SELECT)
-- -------------------------

-- 21. Selecionar nome e gênero de todos os animes
SELECT name, genres FROM anime;

-- 22. Selecionar apenas o nome dos usuários
SELECT username FROM user_details;

-- 23. Selecionar título e pontuação de animes avaliados
SELECT anime_title, rating FROM user_score JOIN anime ON user_score.anime_id = anime.anime_id;

-- -------------------------
-- CONSULTAS: FILTROS (WHERE)
-- -------------------------

-- 24. Selecionar usuários cujo gênero é masculino
SELECT * FROM user_details WHERE gender = 'Male';

-- 25. Selecionar animes cuja URL da imagem começa com 'https'
SELECT name, image_url FROM anime WHERE image_url LIKE 'https%';

-- 26. Selecionar os animes assistidos por um usuário específico
SELECT anime_title FROM user_score WHERE user_id = 68110;

-- 27. Selecionar os animes que o usuário deu nota maior que 3
SELECT anime_title FROM user_score WHERE user_id = 68110 AND rating > 3;

-- 28. Selecionar nome dos animes com nota maior que 3 por usuário X
SELECT a.name 
FROM user_score us
JOIN anime a ON us.anime_id = a.anime_id
WHERE us.user_id = 68110 AND us.rating > 3;

-- 29. Selecionar usuários que se cadastraram após 2015
SELECT * FROM user_details 
WHERE TO_DATE(joined, 'YYYY-MM-DD"T"HH24:MI:SS"+00:00"') > '2015-01-01';

-- 30. Extrair apenas o domínio da URL da imagem
SELECT name, SUBSTRING(image_url FROM 'https?://([^/]+)') AS domain
FROM anime;

-- -------------------------
-- CONSULTAS: JUNÇÕES (JOIN)
-- -------------------------

-- 31. Obter nome do anime e nota que cada usuário deu
SELECT u.username, a.name, s.rating
FROM user_score s
JOIN user_details u ON s.user_id = u.Mal_ID
JOIN anime a ON s.anime_id = a.anime_id;

-- 32. Obter todos os animes assistidos por usuários do gênero feminino
SELECT DISTINCT a.name
FROM user_details u
JOIN user_score s ON u.Mal_ID = s.user_id
JOIN anime a ON s.anime_id = a.anime_id
WHERE u.gender = 'Female';

-- 33. Média de nota por anime
SELECT a.name, AVG(s.rating) AS media_nota
FROM anime a
JOIN user_score s ON a.anime_id = s.anime_id
GROUP BY a.name;

-- 34. Média de nota por usuário
SELECT u.username, AVG(s.rating) AS media
FROM user_details u
JOIN user_score s ON u.Mal_ID = s.user_id
GROUP BY u.username;

-- -------------------------
-- CONSULTAS COMPLEXAS (JOIN + WHERE + AGGREGATE)
-- -------------------------

-- 35. Usuários que assistiram mais de 10 animes e sua média de nota
SELECT u.username, COUNT(s.anime_id) AS total_animes, AVG(s.rating) AS media_score
FROM user_details u
JOIN user_score s ON u.Mal_ID = s.user_id
GROUP BY u.username
HAVING COUNT(s.anime_id) > 10;

-- 36. Animes do gênero "Action" com média de score por usuários homens
SELECT a.name, AVG(s.rating) AS media_score
FROM user_details u
JOIN user_score s ON u.Mal_ID = s.user_id
JOIN anime a ON s.anime_id = a.anime_id
WHERE u.gender = 'Male' AND a.genres ILIKE '%Action%'
GROUP BY a.name;

-- 37. Média de score para cada faixa etária estimada com base no campo Birthday (se possível)
-- (Assumindo birthday como 'YYYY-MM-DD' e idade atual em 2025)
SELECT 
  EXTRACT(YEAR FROM AGE(TO_DATE(birthday, 'YYYY-MM-DD'))) AS idade,
  AVG(s.rating) AS media_score
FROM user_details u
JOIN user_score s ON u.Mal_ID = s.user_id
WHERE birthday IS NOT NULL
GROUP BY idade
ORDER BY idade;

-- 38. Top 5 animes mais populares com maior média de score
SELECT a.name, a.popularity, AVG(s.rating) AS media
FROM anime a
JOIN user_score s ON a.anime_id = s.anime_id
GROUP BY a.anime_id
ORDER BY a.popularity ASC, media DESC
LIMIT 5;

-- 39. Quantidade de usuários por gênero e score médio que atribuem
SELECT gender, COUNT(*) AS qtd_usuarios, AVG(s.rating) AS media_score
FROM user_details u
JOIN user_score s ON u.Mal_ID = s.user_id
WHERE gender IS NOT NULL
GROUP BY gender;

-- 40. Animes lançados antes de 2010 com nota média maior que 8
SELECT a.name, AVG(s.rating) AS media
FROM anime a
JOIN user_score s ON a.anime_id = s.anime_id
WHERE a.aired ~ '([0-9]{4})' AND CAST(SUBSTRING(a.aired FROM '([0-9]{4})') AS INT) < 2010
GROUP BY a.name
HAVING AVG(s.rating) > 8;


-- 41 Média de idade dos usuários que assistiram um anime específico (ex: anime_id = 1):
SELECT AVG(EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM TO_DATE(Birthday, 'YYYY-MM-DD'))) AS media_idade
FROM user_details ud
JOIN user_score us ON ud.Mal_ID = us.user_id
WHERE us.anime_id = 1;


-- 42 Contar usuarios por genero:
SELECT Gender, COUNT(*) AS total
FROM user_details
GROUP BY Gender;


-- 43 Contar usuarios que entraram apos 2020:
SELECT * FROM user_details
WHERE TO_DATE(Joined, 'YYYY-MM-DD') > '2020-01-01';


-- 44 Extrair apenas o https://... da image_url:
SELECT SUBSTRING(image_url FROM 'https://[^ ]*') AS url_limpa
FROM anime;


-- 45 Recuperar todos os animes que o usuário “Guilherme” deu nota:
SELECT a.*
FROM anime a
JOIN user_score us ON a.anime_id = us.anime_id
JOIN user_details ud ON ud.Mal_ID = us.user_id
WHERE ud.Username = 'Guilherme';


-- 46 Animes que “Guilherme” deu nota maior que 3:
SELECT a.*
FROM anime a
JOIN user_score us ON a.anime_id = us.anime_id
JOIN user_details ud ON ud.Mal_ID = us.user_id
WHERE ud.Username = 'Guilherme' AND us.rating > 3;

-- 47 Apenas nomes dos animes que “Guilherme” deu nota maior que 3:
SELECT a.name
FROM anime a
JOIN user_score us ON a.anime_id = us.anime_id
JOIN user_details ud ON ud.Mal_ID = us.user_id
WHERE ud.Username = 'Guilherme' AND us.rating > 3;


-- 48 Consulta com as 3 tabelas:
SELECT a.name, us.rating, ud.Username
FROM anime a
JOIN user_score us ON a.anime_id = us.anime_id
JOIN user_details ud ON us.user_id = ud.Mal_ID;


-- 49 Pessoas de 25 anos e média das notas que deram a animes do gênero “Action”:

SELECT AVG(us.rating) AS media_rating
FROM user_details ud
JOIN user_score us ON ud.Mal_ID = us.user_id
JOIN anime a ON a.anime_id = us.anime_id
WHERE EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM TO_DATE(ud.Birthday, 'YYYY-MM-DD')) = 25
  AND a.genres ILIKE '%Action%';

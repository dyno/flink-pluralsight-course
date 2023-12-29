-- https://duckdb.org/docs/data/csv/overview

CREATE TABLE ratings AS
SELECT *
FROM read_csv('src/main/resources/ml-latest-small/ratings.csv', delim=',', header=TRUE, auto_detect=TRUE);


CREATE TABLE movies AS
SELECT *
FROM read_csv('src/main/resources/ml-latest-small/movies.csv', delim=',', header=TRUE, auto_detect=TRUE);


SELECT m.title,
       sum(rating) / count(rating) AS average_rating,
       count(rating) AS reviews
FROM ratings r
JOIN movies m ON r.movieId = m.movieId
GROUP BY m.title
HAVING reviews > 50
ORDER BY average_rating DESC
LIMIT 10;

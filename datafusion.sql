-- https://arrow.apache.org/datafusion/user-guide/cli.html#creating-external-tables

CREATE EXTERNAL TABLE ratings STORED AS CSV WITH HEADER ROW LOCATION 'src/main/resources/ml-latest-small/ratings.csv';


CREATE EXTERNAL TABLE movies STORED AS CSV WITH HEADER ROW LOCATION 'src/main/resources/ml-latest-small/movies.csv';


SELECT m.title,
       sum(rating) / count(rating) AS average_rating,
       count(rating) AS reviews
FROM ratings r
JOIN movies m ON r."movieId" = m."movieId"
GROUP BY m.title
HAVING reviews > 50
ORDER BY average_rating DESC
LIMIT 10;

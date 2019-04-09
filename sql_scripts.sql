Task 1

DELETE FROM cities WHERE id IN
(SELECT MAX(id) FROM cities
GROUP BY name
HAVING COUNT(name) > 1);


Task 2 

SELECT country, city, citizenship
FROM (SELECT country,
             city,
             ROUND(citizen / SUM(citizen) OVER(PARTITION BY country) * 100, 2) AS citizenship,
             ROW_NUMBER () OVER(PARTITION BY country ORDER BY citizen DESC) AS row_number
      FROM population ORDER BY citizenship) AS citizenship_data
WHERE row_number <= 3
GROUP BY country,citizenship, city
ORDER BY country, citizenship DESC

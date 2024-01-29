SELECT 
    genre, 
    ROUND(SUM("worldwide_sales_millions_#4"),2) AS total_sales_millions
FROM "video-game-sales-database"."video-game-sales-report"
GROUP BY genre
ORDER BY total_sales_millions DESC
LIMIT 5;
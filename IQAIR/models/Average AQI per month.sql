SELECT 
    EXTRACT(MONTH FROM "timestamp") AS month,
    AVG(aqi) AS avg_aqi
FROM 
    air_quality_data
WHERE 
    EXTRACT(YEAR FROM "timestamp") = EXTRACT(YEAR FROM NOW())  -- ใช้ปีปัจจุบัน
GROUP BY 
    month
ORDER BY 
    month;


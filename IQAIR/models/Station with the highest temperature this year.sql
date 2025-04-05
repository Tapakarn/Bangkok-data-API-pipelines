SELECT 
    station_name,
    MAX(temperature) AS max_temperature
FROM 
    air_quality_data
WHERE 
    EXTRACT(YEAR FROM timestamp) = EXTRACT(YEAR FROM NOW())  -- ปีปัจจุบัน
GROUP BY 
    station_name
ORDER BY 
    max_temperature DESC
LIMIT 1;

SELECT 
    station_name,
    MAX(aqi) AS max_aqi
FROM 
    air_quality_data
WHERE 
    timestamp >= NOW() - INTERVAL '1 MONTH'
GROUP BY 
    station_name
ORDER BY 
    max_aqi DESC
LIMIT 1;

SELECT 
    station_name,
    MIN(aqi) AS min_aqi
FROM 
    air_quality_data
GROUP BY 
    station_name
ORDER BY 
    min_aqi ASC
LIMIT 1;

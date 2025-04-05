SELECT 
    COUNT(DISTINCT station_name) AS stations_above_threshold
FROM 
    air_quality_data
WHERE 
    pm25 > 35;  -- กำหนดค่าของ PM2.5 ที่เกินเกณฑ์ (35 µg/m³)

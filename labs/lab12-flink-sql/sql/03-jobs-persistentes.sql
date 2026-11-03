-- Lab 12 · 03 · Jobs persistentes: INSERT INTO lanza un job que sigue corriendo en el clúster Flink

SET 'pipeline.name' = 'alertas-velocidad';
INSERT INTO alertas_velocidad
SELECT vehiculo_id, velocidad, ts_evento
FROM posiciones
WHERE velocidad > 120;

SET 'pipeline.name' = 'estado-vehiculo';
INSERT INTO estado_vehiculo
SELECT vehiculo_id,
       LAST_VALUE(velocidad)   AS ultima_velocidad,
       LAST_VALUE(combustible) AS combustible,
       COUNT(*)                AS lecturas
FROM posiciones
GROUP BY vehiculo_id;

SET 'pipeline.name' = 'velocidad-1min';
INSERT INTO velocidad_1min
SELECT vehiculo_id, window_start, window_end,
       AVG(CAST(velocidad AS DOUBLE)),
       MAX(velocidad),
       COUNT(*)
FROM TABLE(TUMBLE(TABLE posiciones, DESCRIPTOR(ts_evento), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, vehiculo_id;

SHOW JOBS;

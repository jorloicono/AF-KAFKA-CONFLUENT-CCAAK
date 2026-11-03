-- Lab 12 · 02 · Consultas interactivas (se ejecutan, muestran resultados y se cancelan con Ctrl+C / Q)

SHOW TABLES;
DESCRIBE posiciones;

-- Filtro
SELECT vehiculo_id, velocidad, ts_evento FROM posiciones WHERE velocidad > 120;

-- Ventana tumbling de 1 minuto sobre tiempo de evento (función de tabla TUMBLE)
SELECT window_start, window_end, vehiculo_id,
       AVG(CAST(velocidad AS DOUBLE)) AS media,
       MAX(velocidad)                 AS maxima,
       COUNT(*)                       AS lecturas
FROM TABLE(TUMBLE(TABLE posiciones, DESCRIPTOR(ts_evento), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, vehiculo_id;

-- Join con el maestro de vehículos (resultado que se actualiza)
SELECT p.vehiculo_id, v.matricula, v.base, p.velocidad
FROM posiciones p
LEFT JOIN vehiculos v ON p.vehiculo_id = v.vehiculo_id;

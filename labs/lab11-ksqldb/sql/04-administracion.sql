-- Lab 11 · 04 · Administración de ksqlDB
SHOW QUERIES;
SHOW TOPICS;
DESCRIBE estado_vehiculo EXTENDED;
SHOW PROPERTIES;

-- Sustituye <ID> por el identificador real que aparece en SHOW QUERIES (por ejemplo CSAS_EXCESOS_VELOCIDAD_3)
-- EXPLAIN <ID>;
-- TERMINATE <ID>;

-- Borrar un objeto y su topic subyacente (primero hay que terminar las consultas que escriben en él)
-- DROP STREAM excesos_velocidad DELETE TOPIC;

-- Log de procesamiento: errores de deserialización, etc.
-- Ejecútalo DESPUÉS de enviar el mensaje defectuoso del paso 5: si no hay errores,
-- la consulta se queda esperando indefinidamente.
-- SET 'auto.offset.reset' = 'earliest';
-- SELECT * FROM KSQL_PROCESSING_LOG EMIT CHANGES LIMIT 3;

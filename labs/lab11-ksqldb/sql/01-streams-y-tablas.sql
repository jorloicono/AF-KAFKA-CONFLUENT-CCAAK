-- Lab 11 · 01 · Declarar un stream y una tabla sobre topics existentes
SET 'auto.offset.reset' = 'earliest';

CREATE STREAM posiciones (
  vehiculo_id VARCHAR KEY,
  ts          BIGINT,
  lat         DOUBLE,
  lon         DOUBLE,
  velocidad   INT,
  combustible INT
) WITH (KAFKA_TOPIC = 'flota.posiciones', VALUE_FORMAT = 'JSON', TIMESTAMP = 'ts');

CREATE TABLE vehiculos (
  vehiculo_id VARCHAR PRIMARY KEY,
  matricula   VARCHAR,
  modelo      VARCHAR,
  base        VARCHAR
) WITH (KAFKA_TOPIC = 'flota.vehiculos', VALUE_FORMAT = 'JSON');

SHOW STREAMS;
SHOW TABLES;
DESCRIBE posiciones;

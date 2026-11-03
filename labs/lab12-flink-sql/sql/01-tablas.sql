-- Lab 12 · 01 · Tablas de Flink SQL sobre topics de Kafka (se carga al abrir el cliente con -i)
SET 'sql-client.execution.result-mode' = 'tableau';

-- Origen: posiciones de la flota (append-only), con tiempo de evento y watermark
CREATE TABLE posiciones (
  vehiculo_id  STRING,
  ts           BIGINT,
  lat          DOUBLE,
  lon          DOUBLE,
  velocidad    INT,
  combustible  INT,
  ts_evento AS TO_TIMESTAMP_LTZ(ts, 3),
  WATERMARK FOR ts_evento AS ts_evento - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'flota.posiciones',
  'properties.bootstrap.servers' = 'kafka1:29092',
  'properties.group.id' = 'flink-flota',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Maestro de vehículos: topic compactado leído como tabla con clave (upsert)
CREATE TABLE vehiculos (
  vehiculo_id STRING,
  matricula   STRING,
  modelo      STRING,
  base        STRING,
  PRIMARY KEY (vehiculo_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'flota.vehiculos',
  'properties.bootstrap.servers' = 'kafka1:29092',
  'key.format' = 'raw',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

-- Destino append-only: alertas de velocidad
CREATE TABLE alertas_velocidad (
  vehiculo_id STRING,
  velocidad   INT,
  ts_evento   TIMESTAMP_LTZ(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'flota.alertas.flink',
  'properties.bootstrap.servers' = 'kafka1:29092',
  'format' = 'json'
);

-- Destino con actualizaciones por clave: estado de cada vehículo
CREATE TABLE estado_vehiculo (
  vehiculo_id      STRING,
  ultima_velocidad INT,
  combustible      INT,
  lecturas         BIGINT,
  PRIMARY KEY (vehiculo_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'flota.estado.flink',
  'properties.bootstrap.servers' = 'kafka1:29092',
  'key.format' = 'raw',
  'value.format' = 'json'
);

-- Destino append-only: agregados por ventana de 1 minuto
CREATE TABLE velocidad_1min (
  vehiculo_id  STRING,
  window_start TIMESTAMP(3),
  window_end   TIMESTAMP(3),
  media        DOUBLE,
  maxima       INT,
  lecturas     BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'flota.velocidad.1min',
  'properties.bootstrap.servers' = 'kafka1:29092',
  'format' = 'json'
);

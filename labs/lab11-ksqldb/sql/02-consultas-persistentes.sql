-- Lab 11 · 02 · Consultas persistentes (cada una es una aplicación Kafka Streams)
SET 'auto.offset.reset' = 'earliest';

-- Filtro: excesos de velocidad
CREATE STREAM excesos_velocidad WITH (KAFKA_TOPIC = 'flota.excesos', PARTITIONS = 3) AS
  SELECT vehiculo_id,
         velocidad,
         lat,
         lon,
         TIMESTAMPTOSTRING(ts, 'yyyy-MM-dd HH:mm:ss', 'Europe/Madrid') AS hora
  FROM posiciones
  WHERE velocidad > 120
  EMIT CHANGES;

-- Enriquecimiento: join stream-tabla (exige co-particionado: misma clave y mismo nº de particiones)
CREATE STREAM posiciones_enriquecidas WITH (KAFKA_TOPIC = 'flota.posiciones.enriquecidas') AS
  SELECT p.vehiculo_id AS vehiculo_id,
         v.matricula,
         v.modelo,
         v.base,
         p.velocidad,
         p.combustible
  FROM posiciones p
  LEFT JOIN vehiculos v ON p.vehiculo_id = v.vehiculo_id
  EMIT CHANGES;

-- Agregación con ventana: velocidad por vehículo en ventanas de 5 minutos
CREATE TABLE velocidad_5min WITH (KAFKA_TOPIC = 'flota.velocidad.5min') AS
  SELECT vehiculo_id,
         COUNT(*)       AS lecturas,
         AVG(velocidad) AS media,
         MAX(velocidad) AS maxima
  FROM posiciones
  WINDOW TUMBLING (SIZE 5 MINUTES)
  GROUP BY vehiculo_id
  EMIT CHANGES;

-- Estado más reciente de cada vehículo (tabla materializada consultable con pull queries)
CREATE TABLE estado_vehiculo AS
  SELECT vehiculo_id,
         LATEST_BY_OFFSET(combustible) AS combustible,
         LATEST_BY_OFFSET(lat)         AS lat,
         LATEST_BY_OFFSET(lon)         AS lon,
         COUNT(*)                      AS lecturas
  FROM posiciones
  GROUP BY vehiculo_id
  EMIT CHANGES;

-- Agregación con cambio de clave: excesos por base (provoca un topic de repartición)
CREATE TABLE excesos_por_base AS
  SELECT base,
         COUNT(*) AS excesos
  FROM posiciones_enriquecidas
  WHERE velocidad > 120 AND base IS NOT NULL
  GROUP BY base
  EMIT CHANGES;

SHOW QUERIES;

-- Lab 11 · 03 · Pull queries (foto del estado actual) y push queries (suscripción continua)

-- Pull: responde y termina
SELECT * FROM estado_vehiculo WHERE vehiculo_id = 'V001';
SELECT * FROM velocidad_5min WHERE vehiculo_id = 'V003';
SELECT * FROM excesos_por_base;

-- Push: se queda escuchando cambios (LIMIT para que termine sola)
SELECT * FROM excesos_velocidad EMIT CHANGES LIMIT 5;
SELECT * FROM posiciones_enriquecidas EMIT CHANGES LIMIT 5;

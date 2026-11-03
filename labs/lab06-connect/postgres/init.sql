-- Tabla de origen: mantenimientos de los vehículos de la flota
CREATE TABLE mantenimientos (
  id           SERIAL PRIMARY KEY,
  vehiculo_id  VARCHAR(10)  NOT NULL,
  tipo         VARCHAR(40)  NOT NULL,
  coste_euros  INTEGER      NOT NULL,
  actualizado  TIMESTAMP    NOT NULL DEFAULT now()
);

INSERT INTO mantenimientos (vehiculo_id, tipo, coste_euros) VALUES
  ('V001', 'Cambio de aceite', 120),
  ('V002', 'Neumáticos', 640),
  ('V003', 'Frenos', 310),
  ('V004', 'ITV', 55),
  ('V005', 'Cambio de aceite', 120),
  ('V006', 'Batería', 180),
  ('V007', 'Revisión general', 450),
  ('V008', 'Neumáticos', 640),
  ('V009', 'Embrague', 900),
  ('V010', 'ITV', 55);

# Lab 11 · ksqlDB: streams, tablas y consultas persistentes sobre los datos de la flota

**Bloque 9 · Duración estimada: 75 minutos**

## Objetivo

El examen de administrador no te va a pedir que escribas consultas complejas, pero sí que entiendas qué es ksqlDB por dentro, qué crea en el clúster y cómo se administra. Por eso este laboratorio tiene dos partes: primero construimos un pequeño flujo de procesamiento sobre la flota (filtro, enriquecimiento con un join, agregaciones con y sin ventana), y después lo miramos con ojos de administrador: qué consultas persistentes hay, qué topics internos han aparecido, qué es el `service.id`, dónde van los errores y cómo se para y se borra una consulta.

## Punto de partida

```bash
cd labs/lab11-ksqldb
docker compose up -d
sleep 30
```

Crea los dos topics con el mismo número de particiones (es requisito para el join, como veremos) y carga los datos. Deja después el generador en modo continuo en una terminal aparte:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --topic flota.posiciones --partitions 3 --replication-factor 1
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --topic flota.vehiculos --partitions 3 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka1 bash /scripts/cargar-vehiculos.sh
docker exec kafka1 bash /scripts/generar-flota.sh 2000
docker exec -it kafka1 bash /scripts/generar-flota.sh 0
```

En otra terminal, abre el cliente de ksqlDB:

```bash
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

## Paso 1 · Streams y tablas

Ejecuta el primer script desde el cliente (o copia las sentencias una a una si prefieres ir despacio):

```sql
RUN SCRIPT '/sql/01-streams-y-tablas.sql';
```

Un *stream* es una secuencia inmutable de eventos: cada posición es un hecho nuevo. Una *tabla* es el estado más reciente por clave: cada mensaje de `flota.vehiculos` actualiza la ficha de un vehículo. Por eso la tabla se apoya en un topic compactado y declara una `PRIMARY KEY`, mientras que el stream declara una columna `KEY`. Mira el contenido de ambos con consultas *push* que terminan solas:

```sql
SET 'auto.offset.reset' = 'earliest';
SELECT * FROM posiciones EMIT CHANGES LIMIT 5;
SELECT * FROM vehiculos EMIT CHANGES LIMIT 5;
```

## Paso 2 · Consultas persistentes

```sql
RUN SCRIPT '/sql/02-consultas-persistentes.sql';
```

Cada sentencia `CREATE ... AS SELECT` crea una consulta persistente: una aplicación Kafka Streams que corre dentro del servidor ksqlDB de forma indefinida, lee de uno o varios topics y escribe el resultado en un topic nuevo. Hemos creado cinco: un filtro de excesos de velocidad, un enriquecimiento con un join stream-tabla, una agregación con ventana *tumbling* de cinco minutos, el estado más reciente de cada vehículo y un recuento de excesos por base.

El join funciona porque los dos topics están co-particionados: misma clave (`vehiculo_id`), mismo número de particiones y el mismo particionador. Si `flota.vehiculos` tuviera un número de particiones distinto, ksqlDB rechazaría el join. Es un detalle que aparece en preguntas de examen.

## Paso 3 · Pull y push queries

```sql
RUN SCRIPT '/sql/03-pull-y-push.sql';
```

Las *pull queries* consultan el estado materializado de una tabla, como un `SELECT` en una base de datos, y terminan. Las *push queries* (`EMIT CHANGES`) se suscriben y van devolviendo resultados a medida que llegan eventos. Repite varias veces la pull query de `estado_vehiculo` y verás cómo cambian el combustible y la posición.

## Paso 4 · Lo que ksqlDB ha creado en el clúster

Sal del cliente (`exit`) y lista los topics desde el broker:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --list
```

Además de los topics de salida que hemos nombrado (`flota.excesos`, `flota.posiciones.enriquecidas`, `flota.velocidad.5min`...), verás varios tipos de topics internos, todos con el prefijo `_confluent-ksql-flota_`, que viene del `ksql.service.id`. El topic `_confluent-ksql-flota_command_topic` guarda todas las sentencias DDL y es la fuente de verdad del servidor: si se reinicia, reconstruye todas sus consultas leyéndolo. Los topics `...-changelog` respaldan los almacenes de estado (RocksDB) de las agregaciones, y los `...-repartition` aparecen cuando una consulta cambia la clave, como en `GROUP BY base`. Todos los servidores ksqlDB que comparten `ksql.service.id` forman un mismo clúster y se reparten el trabajo.

## Paso 5 · Administración

Vuelve al cliente y ejecuta:

```sql
RUN SCRIPT '/sql/04-administracion.sql';
```

Con el identificador de la consulta de excesos que veas en `SHOW QUERIES`, prueba `EXPLAIN` (muestra la topología de Kafka Streams y los topics que usa), `TERMINATE` para pararla y `DROP STREAM excesos_velocidad DELETE TOPIC;` para borrar el stream y su topic.

Ahora provoca un error de deserialización enviando un mensaje que no es JSON al topic de posiciones, y búscalo en el log de procesamiento:

```bash
docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:29092 --topic flota.posiciones \
  --property parse.key=true --property key.separator='|' <<< 'V001|esto no es json'
```

```sql
SELECT * FROM KSQL_PROCESSING_LOG EMIT CHANGES LIMIT 1;
```

ksqlDB no se detiene ante un mensaje defectuoso: lo salta y deja la traza en el log de procesamiento, que conviene vigilar igual que la DLQ de Connect.

Por último, consulta la API REST del servidor desde tu equipo:

```bash
curl -s localhost:8088/info ; echo
curl -s localhost:8088/healthcheck ; echo
```

## Preguntas de comprobación

1. ¿Qué diferencia hay entre un stream y una tabla en ksqlDB, y qué tipo de topic suele respaldar a una tabla?
2. ¿Qué es una consulta persistente y dónde se ejecuta?
3. ¿Qué tres condiciones exige un join entre un stream y una tabla?
4. ¿Para qué sirve el *command topic* y qué pasaría si se borrara?
5. ¿Qué tienen en común dos servidores ksqlDB con el mismo `ksql.service.id`?

## Limpieza

```bash
docker compose down -v
```

# Lab 12 · Flink SQL sobre los topics de la flota

**Bloque 9 · Duración estimada: 60–75 minutos**

## Objetivo

Apache Flink es hoy la apuesta de Confluent para el procesamiento de streams, tanto en Confluent Cloud como en Confluent Platform. En este laboratorio levantamos un clúster Flink mínimo (un JobManager y un TaskManager), declaramos tablas de Flink SQL sobre los mismos topics de la flota que usamos con ksqlDB, lanzamos consultas interactivas y jobs persistentes, y comprobamos cómo los checkpoints permiten que un job sobreviva a la caída de un TaskManager. Al terminar tendrás los elementos para comparar con criterio ksqlDB y Flink.

## Punto de partida

Si no lo hiciste al preparar el curso, descarga el conector de Kafka para Flink desde la raíz del repositorio:

```bash
bash comun/scripts/descargar-dependencias.sh
cd labs/lab12-flink-sql
docker compose up -d
sleep 30
```

Crea los topics, carga el maestro de vehículos y deja el generador continuo en una terminal aparte, porque las ventanas de tiempo de evento solo se cierran cuando sigue llegando información:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --topic flota.posiciones --partitions 3 --replication-factor 1
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --topic flota.vehiculos --partitions 3 --replication-factor 1 --config cleanup.policy=compact
docker exec kafka1 bash /scripts/cargar-vehiculos.sh
docker exec -it kafka1 bash /scripts/generar-flota.sh 0
```

Abre la interfaz web de Flink en http://localhost:8081: verás un TaskManager con 4 slots libres y ningún job.

## Paso 1 · Abrir el cliente SQL

En otra terminal:

```bash
docker compose run --rm sql-client
```

El cliente arranca ejecutando `01-tablas.sql`, que declara cinco tablas. Ninguna de ellas guarda datos: son definiciones que conectan Flink con topics de Kafka. Compruébalo:

```sql
SHOW TABLES;
DESCRIBE posiciones;
```

Fíjate en dos detalles de `posiciones`. La columna calculada `ts_evento` convierte el campo `ts` en una marca de tiempo, y la cláusula `WATERMARK` le dice a Flink que use esa columna como tiempo de evento y que espere hasta 5 segundos a los eventos que lleguen desordenados. Y `vehiculos` usa el conector `upsert-kafka` con clave primaria: Flink interpreta cada mensaje como un alta o una actualización de la fila con esa clave, que es exactamente la semántica de un topic compactado.

## Paso 2 · Consultas interactivas

Copia las consultas de `sql/02-consultas-interactivas.sql` una a una. Cada `SELECT` lanza un job temporal en el clúster que va mostrando resultados hasta que lo cancelas (pulsa `Q` o `Ctrl+C`). Mientras corren, mira la interfaz web: cada consulta aparece como un job con su grafo de operadores. La consulta de ventana no mostrará filas hasta que pase el primer minuto completo y el watermark lo supere; es el comportamiento correcto del tiempo de evento.

## Paso 3 · Jobs persistentes

Los `INSERT INTO` no muestran resultados: envían un job al clúster que sigue corriendo aunque cierres el cliente. Copia las sentencias de `sql/03-jobs-persistentes.sql` y después:

```sql
SHOW JOBS;
```

Comprueba desde otra terminal que los topics de salida reciben datos:

```bash
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 --topic flota.alertas.flink --max-messages 3
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 --topic flota.estado.flink --max-messages 3 --property print.key=true
```

Observa la diferencia entre los dos tipos de destino. `alertas_velocidad` es *append-only*: cada alerta es un mensaje nuevo. `estado_vehiculo` es una tabla que se actualiza: como la consulta agrupa por `vehiculo_id`, el resultado de cada vehículo cambia con cada evento, y por eso el destino tiene que ser `upsert-kafka` con clave primaria. Si intentas escribir un resultado que se actualiza en una tabla `kafka` normal, Flink lo rechaza al planificar la consulta.

## Paso 4 · Checkpoints y tolerancia a fallos

En la interfaz web, entra en el job `estado-vehiculo` y abre la pestaña *Checkpoints*: hay uno completado cada 10 segundos (`execution.checkpointing.interval`). Cada checkpoint guarda de forma consistente el estado de los operadores y los offsets de Kafka que Flink ha leído. Ahora mata el TaskManager:

```bash
docker kill taskmanager
docker compose up -d taskmanager
```

El job pasa por `RESTARTING` y vuelve a `RUNNING` cuando el TaskManager regresa, recuperando el estado y los offsets del último checkpoint. Los contadores de `lecturas` del topic `flota.estado.flink` continúan desde donde estaban en vez de empezar de cero. Un detalle importante para la administración: Flink no depende de los offsets confirmados en el grupo de consumidores de Kafka para recuperarse, sino de sus propios checkpoints.

## Paso 5 · Parar un job con savepoint

Un *savepoint* es un checkpoint lanzado manualmente para parar un job de forma controlada (por ejemplo, para actualizar la consulta o la versión de Flink) y retomarlo después. En el cliente SQL, con el identificador de `SHOW JOBS`:

```sql
STOP JOB '<id-del-job>' WITH SAVEPOINT;
```

El cliente devuelve la ruta del savepoint, que está dentro del volumen `checkpoints`.

## Paso 6 · ksqlDB o Flink

Con lo que has visto en este laboratorio y en el anterior, rellena la comparación y coméntala en clase:

| Aspecto | ksqlDB | Flink |
|---|---|---|
| Modelo de ejecución | | |
| Dónde vive el estado | | |
| Lenguaje y alcance | | |
| Fuentes y destinos | | |
| Estrategia de Confluent | | |

En resumen: ksqlDB está construido sobre Kafka Streams, solo lee y escribe Kafka, guarda el estado en RocksDB respaldado por topics *changelog* y es muy sencillo de operar para casos acotados; Confluent lo mantiene pero ya no es su línea principal. Flink es un motor de procesamiento distribuido independiente, con SQL estándar, APIs de Java y Python, procesamiento por lotes y streaming, muchos conectores además de Kafka, estado gestionado con checkpoints, y es la opción estratégica de Confluent (Confluent Cloud for Apache Flink y Confluent Platform for Apache Flink). A cambio, operar un clúster Flink propio es más exigente.

## Preguntas de comprobación

1. ¿Qué papel cumple el watermark en una agregación por ventana de tiempo de evento?
2. ¿Por qué `estado_vehiculo` necesita el conector `upsert-kafka` y `alertas_velocidad` no?
3. ¿Qué guarda un checkpoint de un job que lee de Kafka?
4. ¿Qué diferencia hay entre un checkpoint y un savepoint?
5. ¿En qué tipo de caso de uso elegirías ksqlDB y en cuál Flink?

## Limpieza

```bash
docker compose --profile cliente down -v
```

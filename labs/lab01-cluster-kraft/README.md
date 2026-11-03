# Lab 1 · Clúster KRaft de 3 brokers, recorrido por la CLI y caída de un broker

**Bloque 2 · Duración estimada: 60–75 minutos**

## Objetivo

En este laboratorio vamos a levantar un clúster de Kafka en modo KRaft con tres nodos que hacen a la vez de broker y de controller, y lo vamos a usar para recorrer las herramientas de línea de comandos que más aparecen en el examen CCAAK. Al final provocaremos la caída de un broker y observaremos qué pasa con los líderes, con el ISR y con el quórum de controladores, que es exactamente el tipo de razonamiento que el examen pide en las preguntas de escenario.

## Punto de partida

Necesitas Docker Desktop (o Docker Engine con el plugin compose) con al menos 6 GB de RAM asignados. Todos los comandos de Kafka se ejecutan dentro de los contenedores, así que no hace falta instalar nada más en tu equipo.

```bash
cd labs/lab01-cluster-kraft
docker compose up -d
docker compose ps
docker logs kafka1 2>&1 | grep -i "Kafka Server started"
```

Cuando los tres contenedores estén en estado `running` y veas el mensaje de arranque en los logs, abre una shell dentro del primer nodo. A partir de aquí, salvo que se diga lo contrario, todos los comandos se lanzan desde esa shell.

```bash
docker exec -it kafka1 bash
```

## Paso 1 · El quórum KRaft

Lo primero es comprobar quién manda en el clúster. En KRaft los metadatos viven en un topic interno llamado `__cluster_metadata` que se replica con el protocolo Raft entre los controllers votantes. La herramienta `kafka-metadata-quorum` nos dice quién es el líder del quórum y cómo de al día van los demás.

```bash
kafka-metadata-quorum --bootstrap-server kafka1:29092 describe --status
kafka-metadata-quorum --bootstrap-server kafka1:29092 describe --replication
```

Fíjate en el campo `LeaderId` (el controller activo), en `HighWatermark` y en la columna `Lag` de la salida de `--replication`. Anota cuál es el controller activo porque lo vamos a necesitar en el paso 5.

## Paso 2 · Crear y describir topics

Vamos a crear el topic principal del caso práctico, las posiciones de la flota, con 6 particiones y factor de replicación 3, y el maestro de vehículos como topic compactado.

```bash
kafka-topics --bootstrap-server kafka1:29092 --create --topic flota.posiciones \
  --partitions 6 --replication-factor 3 --config min.insync.replicas=2

kafka-topics --bootstrap-server kafka1:29092 --create --topic flota.vehiculos \
  --partitions 3 --replication-factor 3 --config cleanup.policy=compact

kafka-topics --bootstrap-server kafka1:29092 --list
kafka-topics --bootstrap-server kafka1:29092 --describe --topic flota.posiciones
```

En la salida de `--describe` cada línea es una partición, y para cada una ves el `Leader`, la lista ordenada de `Replicas` (la primera es el líder preferido) y el `Isr`. Comprueba que los líderes están repartidos entre los tres brokers.

Ahora mira la configuración efectiva del topic, tanto la que hemos fijado como la heredada del broker:

```bash
kafka-configs --bootstrap-server kafka1:29092 --describe --entity-type topics \
  --entity-name flota.posiciones --all | head -40
```

## Paso 3 · Producir y consumir

Cargamos el maestro de vehículos y mil posiciones con los scripts comunes del curso:

```bash
bash /scripts/cargar-vehiculos.sh
bash /scripts/generar-flota.sh 1000
```

Consumimos desde el principio usando un grupo de consumidores y mostrando la clave y la partición de cada mensaje:

```bash
kafka-console-consumer --bootstrap-server kafka1:29092 --topic flota.posiciones \
  --group lab01-lectores --from-beginning --max-messages 20 \
  --property print.key=true --property print.partition=true --property print.offset=true
```

Observa que todos los eventos de un mismo vehículo caen siempre en la misma partición: es la consecuencia de usar `vehiculo_id` como clave. Consulta ahora el estado del grupo y su lag:

```bash
kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group lab01-lectores
kafka-get-offsets --bootstrap-server kafka1:29092 --topic flota.posiciones
```

Como solo hemos leído 20 mensajes, verás un `LAG` distinto de cero en varias particiones. La columna `CONSUMER-ID` aparece vacía porque el consumidor ya ha terminado: el grupo existe y tiene offsets confirmados, pero no tiene miembros activos.

## Paso 4 · Los ficheros en disco

Kafka guarda cada partición como un directorio con segmentos. Vamos a verlo:

```bash
ls -l /var/lib/kafka/data/ | head -30
ls -l /var/lib/kafka/data/flota.posiciones-0/
kafka-log-dirs --bootstrap-server kafka1:29092 --describe --topic-list flota.posiciones
kafka-dump-log --print-data-log --files /var/lib/kafka/data/flota.posiciones-0/00000000000000000000.log | head -20
```

Identifica el fichero `.log` (los datos), el `.index` (offset a posición física), el `.timeindex` (marca de tiempo a offset) y el directorio `__cluster_metadata-0`, que es el log de metadatos de KRaft. Si la partición 0 no está en `kafka1`, prueba con otra partición que sí lo esté.

## Paso 5 · Caída de un broker

Sal de la shell (`exit`) y, desde tu terminal, detén el segundo nodo:

```bash
docker stop kafka2
```

Vuelve a entrar en `kafka1` y observa las consecuencias:

```bash
docker exec -it kafka1 bash
kafka-topics --bootstrap-server kafka1:29092 --describe --topic flota.posiciones
kafka-topics --bootstrap-server kafka1:29092 --describe --under-replicated-partitions
kafka-metadata-quorum --bootstrap-server kafka1:29092 describe --status
```

Lo que deberías ver es que las particiones cuyo líder era el broker 2 tienen ahora un líder nuevo, que el 2 ha desaparecido del `Isr` de todas las particiones y que el quórum sigue funcionando con dos de tres votantes. Si el broker 2 era el controller activo, comprobarás además que otro nodo ha asumido el liderazgo del quórum.

Ahora comprueba que, con `min.insync.replicas=2` y dos réplicas vivas, el clúster sigue aceptando escrituras con `acks=all`:

```bash
bash /scripts/generar-flota.sh 100
```

### Paso 5b (opcional, para entender el quórum)

Detén también el tercer nodo (`docker stop kafka3` desde fuera). Con un solo votante de tres, el quórum no tiene mayoría y no puede haber cambios de metadatos; además, el productor con `acks=all` empezará a fallar con `NOT_ENOUGH_REPLICAS` porque el ISR ya no puede llegar a 2. Intenta producir y observa el error, y después vuelve a arrancar los dos nodos:

```bash
docker start kafka2 kafka3
```

## Paso 6 · Recuperación y líder preferido

Con los tres nodos de nuevo arriba, describe el topic hasta que el ISR vuelva a tener tres réplicas en todas las particiones. Verás que algunos líderes no han vuelto a su réplica preferida. Kafka lo corregirá solo pasado el intervalo de `leader.imbalance.check.interval.seconds` (300 segundos por defecto) si `auto.leader.rebalance.enable=true`, pero podemos forzarlo:

```bash
kafka-leader-election --bootstrap-server kafka1:29092 --election-type PREFERRED --all-topic-partitions
kafka-topics --bootstrap-server kafka1:29092 --describe --topic flota.posiciones
```

## Preguntas de comprobación

1. ¿Qué diferencia hay entre la lista `Replicas` y la lista `Isr`, y cuál de las dos cambia cuando cae un broker?
2. ¿Por qué el clúster siguió aceptando escrituras con un broker caído y dejó de hacerlo con dos?
3. ¿Qué pasaría con un topic de factor de replicación 1 cuyo único líder estuviera en el broker detenido?
4. ¿Cuántos controllers votantes necesitas para tolerar la caída de dos de ellos?

## Limpieza

```bash
docker compose down -v
```

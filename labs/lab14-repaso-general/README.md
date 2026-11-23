# Lab 14 · Repaso general: predecir, comprobar y explicar

**Sesión 13 · Duración estimada: 50 minutos**

## Objetivo

Este último laboratorio no enseña nada nuevo: repasa, sobre un clúster real, los razonamientos que más se repiten en el examen CCAAK. Cada reto empieza con una pregunta tipo examen. **Antes de ejecutar nada, escribe tu predicción.** Después compruébala en el clúster y explica por qué ha pasado lo que ha pasado. Si tu predicción falla, ya sabes qué repasar antes del simulacro.

| Reto | Tema | Dominio CCAAK |
|---|---|---|
| 1 | Durabilidad: replicación, `min.insync.replicas` y `acks` | Fundamentos · Configuración |
| 2 | Estados de una partición enferma | Troubleshooting · Observabilidad |
| 3 | Quórum KRaft y líder preferido | Arquitectura de despliegue |
| 4 | Precedencia de configuraciones | Configuración del clúster |
| 5 | Particiones y factor de replicación | Configuración del clúster |
| 6 | Grupos de consumidores, lag y offsets | Fundamentos · Troubleshooting |
| 7 | Ronda rápida sin clúster: seguridad, Schema Registry, Connect y DR | Seguridad · Connect · Arquitectura |

## Punto de partida

Usamos el clúster de tres nodos del Lab 1. Si tienes otro laboratorio levantado, bájalo antes con `docker compose down -v` en su carpeta.

```bash
cd labs/lab01-cluster-kraft
docker compose up -d
docker compose ps
docker exec -it kafka1 bash
```

Salvo que se indique otra cosa, todos los comandos se ejecutan dentro de `kafka1`. Los `docker stop` y `docker start` se ejecutan en otra terminal, fuera del contenedor.

## Reto 1 · Durabilidad

> **Pregunta.** Un topic tiene factor de replicación 3 y `min.insync.replicas=3`. Cae un broker. ¿Qué le pasa a un productor con `acks=all`? ¿Y a uno con `acks=1`? ¿Y a un consumidor?

Escribe tu predicción. Después crea el topic:

```bash
kafka-topics --bootstrap-server kafka1:29092 --create --topic repaso.durabilidad \
  --partitions 3 --replication-factor 3 --config min.insync.replicas=3
kafka-topics --bootstrap-server kafka1:29092 --create --topic repaso.rf1 \
  --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server kafka1:29092 --describe --topic repaso.rf1
```

El segundo topic, de factor de replicación 1, lo usaremos en el reto 2. Hay que crearlo ahora, con los tres brokers vivos, para que una de sus particiones caiga en el broker 2. Anota cuál es.

Lanza también un consumidor de calentamiento. Así se crea el topic interno `__consumer_offsets`, que tiene factor de replicación 3 y **no se puede crear con un broker caído**:

```bash
kafka-console-consumer --bootstrap-server kafka1:29092 --topic repaso.durabilidad \
  --group repaso-calentamiento --from-beginning --timeout-ms 5000
```

Desde fuera del contenedor, detén un broker:

```bash
docker stop kafka2
```

Vuelve a `kafka1` y produce con `acks=all`. Los tiempos de espera van reducidos para que el error aparezca en segundos:

```bash
echo 'V001|{"vehiculo_id":"V001","velocidad":90}' | kafka-console-producer \
  --bootstrap-server kafka1:29092 --topic repaso.durabilidad \
  --property parse.key=true --property key.separator='|' \
  --producer-property acks=all \
  --producer-property request.timeout.ms=5000 --producer-property delivery.timeout.ms=10000
```

Repite el mismo comando con `acks=1` y lee el topic:

```bash
echo 'V002|{"vehiculo_id":"V002","velocidad":80}' | kafka-console-producer \
  --bootstrap-server kafka1:29092 --topic repaso.durabilidad \
  --property parse.key=true --property key.separator='|' --producer-property acks=1

kafka-console-consumer --bootstrap-server kafka1:29092 --topic repaso.durabilidad \
  --from-beginning --timeout-ms 10000 --property print.key=true
```

**Explica.** ¿Por qué la combinación de referencia es 3, 2 y `all`, y no 3, 3 y `all`? (Lo comprobamos al final del reto 2.)

## Reto 2 · Estados de una partición enferma

> **Pregunta.** Con `kafka2` todavía parado, ¿qué topics aparecerán como *under-replicated*, cuáles por debajo del ISR mínimo y cuáles sin líder?

```bash
kafka-topics --bootstrap-server kafka1:29092 --describe --under-replicated-partitions
kafka-topics --bootstrap-server kafka1:29092 --describe --under-min-isr-partitions
kafka-topics --bootstrap-server kafka1:29092 --describe --unavailable-partitions
```

La primera lista sale larga porque incluye las 50 particiones de `__consumer_offsets`: búscate entre ellas. `repaso.durabilidad` debería salir en las dos primeras, y la partición de `repaso.rf1` que vivía en el broker 2, en la tercera, con `Leader: none`.

Ahora baja `min.insync.replicas` a 2, que es el valor de referencia, y vuelve a producir con `acks=all` con el mismo comando del reto 1:

```bash
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type topics \
  --entity-name repaso.durabilidad --add-config min.insync.replicas=2
kafka-topics --bootstrap-server kafka1:29092 --describe --under-min-isr-partitions
```

**Explica.** ¿Qué diferencia hay entre una partición *under-replicated*, una por debajo del ISR mínimo y una *offline*? ¿Qué decide `unclean.leader.election.enable`?

## Reto 3 · Quórum KRaft y líder preferido

> **Pregunta.** Con tres controllers votantes y uno caído, ¿sigue habiendo controller activo? ¿Cuántos votantes harían falta para tolerar dos caídas?

```bash
kafka-metadata-quorum --bootstrap-server kafka1:29092 describe --status
kafka-metadata-quorum --bootstrap-server kafka1:29092 describe --replication
```

Ahora levanta el broker desde fuera (`docker start kafka2`). Espera a que el ISR vuelva a tener tres réplicas y fíjate en los líderes:

```bash
kafka-topics --bootstrap-server kafka1:29092 --describe --topic repaso.durabilidad
kafka-leader-election --bootstrap-server kafka1:29092 --election-type PREFERRED --all-topic-partitions
kafka-topics --bootstrap-server kafka1:29092 --describe --topic repaso.durabilidad
```

**Explica.** ¿Qué es la réplica preferida y por qué conviene que sea la líder?

## Reto 4 · Precedencia de configuraciones

> **Pregunta.** Configuramos la retención en tres sitios a la vez: un valor por defecto dinámico para todo el clúster de 2 días, un valor dinámico para el broker 1 de 1 día y un valor del topic de 1 hora. ¿Qué retención aplica el topic? ¿Y qué retención aplicaría un topic sin valor propio cuyas particiones lidera el broker 1?

```bash
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type brokers --entity-default \
  --add-config log.retention.ms=172800000
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type brokers --entity-name 1 \
  --add-config log.retention.ms=86400000
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type topics --entity-name repaso.durabilidad \
  --add-config retention.ms=3600000

kafka-configs --bootstrap-server kafka1:29092 --describe --entity-type topics \
  --entity-name repaso.durabilidad --all | grep -E "retention.ms|min.insync"
kafka-configs --bootstrap-server kafka1:29092 --describe --entity-type brokers \
  --entity-name 1 --all | grep "log.retention.ms"
```

Fíjate en la fuente que muestra cada valor (`DYNAMIC_TOPIC_CONFIG`, `DYNAMIC_BROKER_CONFIG`, `DYNAMIC_DEFAULT_BROKER_CONFIG`, `STATIC_BROKER_CONFIG`, `DEFAULT_CONFIG`). Y date cuenta de que la propiedad cambia de nombre: `log.retention.ms` en el broker y `retention.ms` en el topic.

Deja el clúster como estaba:

```bash
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type brokers --entity-name 1 --delete-config log.retention.ms
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type brokers --entity-default --delete-config log.retention.ms
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type topics --entity-name repaso.durabilidad --delete-config retention.ms
```

**Explica.** ¿Qué tres modos de actualización tienen las configuraciones de broker y qué implica cada uno?

## Reto 5 · Particiones y factor de replicación

> **Pregunta.** ¿Puedo pasar `repaso.durabilidad` de 3 a 6 particiones? ¿Y volver de 6 a 3? ¿Con qué comando subo el factor de replicación de `repaso.rf1` de 1 a 3?

```bash
kafka-topics --bootstrap-server kafka1:29092 --alter --topic repaso.durabilidad --partitions 6
kafka-topics --bootstrap-server kafka1:29092 --alter --topic repaso.durabilidad --partitions 3
```

El factor de replicación no se cambia con `kafka-topics`. Se cambia con una reasignación que da más réplicas a cada partición:

```bash
cat > /tmp/rf3.json <<'EOF'
{"version":1,"partitions":[
 {"topic":"repaso.rf1","partition":0,"replicas":[1,2,3]},
 {"topic":"repaso.rf1","partition":1,"replicas":[2,3,1]},
 {"topic":"repaso.rf1","partition":2,"replicas":[3,1,2]}]}
EOF
kafka-reassign-partitions --bootstrap-server kafka1:29092 --reassignment-json-file /tmp/rf3.json --execute
kafka-reassign-partitions --bootstrap-server kafka1:29092 --reassignment-json-file /tmp/rf3.json --verify
kafka-topics --bootstrap-server kafka1:29092 --describe --topic repaso.rf1
```

**Explica.** ¿Qué les pasa a las claves cuando aumentas particiones? ¿Por qué hay que ejecutar siempre `--verify` cuando la reasignación lleva `--throttle`?

## Reto 6 · Grupos de consumidores, lag y offsets

> **Pregunta.** Un grupo ha leído 100 de 1000 mensajes y ya no tiene consumidores. ¿Qué muestra `--describe`? ¿Puedo devolverlo al principio del topic? ¿Y si tuviera un consumidor conectado?

```bash
kafka-topics --bootstrap-server kafka1:29092 --create --topic flota.posiciones \
  --partitions 6 --replication-factor 3 --config min.insync.replicas=2
bash /scripts/generar-flota.sh 1000

kafka-console-consumer --bootstrap-server kafka1:29092 --topic flota.posiciones \
  --group repaso-lectores --from-beginning --max-messages 100 > /dev/null
kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group repaso-lectores
```

Rebobina el grupo. Primero en simulación, después de verdad:

```bash
kafka-consumer-groups --bootstrap-server kafka1:29092 --group repaso-lectores \
  --topic flota.posiciones --reset-offsets --to-earliest --dry-run
kafka-consumer-groups --bootstrap-server kafka1:29092 --group repaso-lectores \
  --topic flota.posiciones --reset-offsets --to-earliest --execute
kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group repaso-lectores
```

**Explica.** Si el lag de un grupo crece sin parar y el clúster está sano, ¿dónde miras primero? ¿Qué dos parámetros del consumidor provocan rebalanceos continuos cuando están mal ajustados?

## Reto 7 · Ronda rápida sin clúster

El formador lanza preguntas cortas sobre lo que no cabe en este clúster: seguridad, Schema Registry, Kafka Connect y replicación entre clústeres. Responde en voz alta o en el chat antes de que dé la solución.

## Limpieza

```bash
exit
cd labs/lab01-cluster-kraft
docker compose down -v
```

## Hoja de predicciones

| Reto | Mi predicción | ¿Acerté? | Qué repasar |
|---|---|---|---|
| 1 | | | |
| 2 | | | |
| 3 | | | |
| 4 | | | |
| 5 | | | |
| 6 | | | |
| 7 | | | |

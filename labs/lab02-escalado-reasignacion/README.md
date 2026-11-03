# Lab 2 · Añadir un broker, reasignar particiones y cambiar configuraciones en caliente

**Bloque 3 · Duración estimada: 60–75 minutos**

## Objetivo

Un clúster de Kafka no se rebalancea solo cuando le añades un broker: el nodo nuevo entra vacío y se queda así hasta que alguien mueve particiones hacia él. En este laboratorio vamos a hacer ese trabajo a mano con `kafka-reassign-partitions`, usando throttling para no saturar la red, vamos a subir el factor de replicación de un topic existente, a añadir particiones, y a cambiar configuraciones de broker y de topic sin reiniciar nada, entendiendo en qué orden de precedencia se aplican. Cerraremos con una demostración opcional de Self-Balancing Clusters, la alternativa automática de Confluent Server.

## Punto de partida

```bash
cd labs/lab02-escalado-reasignacion
docker compose up -d                       # arranca kafka1, kafka2 y kafka3
docker exec -it kafka1 bash
```

Creamos el topic con factor de replicación 2 (a propósito, para subirlo después) y cargamos datos suficientes para que la reasignación tarde unos segundos:

```bash
kafka-topics --bootstrap-server kafka1:29092 --create --topic flota.posiciones \
  --partitions 6 --replication-factor 2
bash /scripts/generar-flota.sh 20000
kafka-topics --bootstrap-server kafka1:29092 --describe --topic flota.posiciones
```

## Paso 1 · Añadir el broker 4

Sal de la shell y arranca el cuarto nodo, que está definido en el `docker-compose.yml` con el perfil `escalado` y con `process.roles=broker`, es decir, es un broker puro que no vota en el quórum KRaft:

```bash
docker compose --profile escalado up -d kafka4
docker exec -it kafka1 bash
kafka-broker-api-versions --bootstrap-server kafka1:29092 | grep "id:"
kafka-topics --bootstrap-server kafka1:29092 --describe --topic flota.posiciones
kafka-log-dirs --bootstrap-server kafka1:29092 --describe --broker-list 4
```

El broker 4 aparece en el clúster, pero no tiene ninguna réplica: `kafka-log-dirs` lo muestra sin particiones. Este es el comportamiento que el examen espera que conozcas.

## Paso 2 · Reasignación manual con throttling

El flujo de `kafka-reassign-partitions` tiene tres fases: generar una propuesta, ejecutarla y verificarla. Primero le pedimos una propuesta que reparta el topic entre los cuatro brokers, y guardamos tanto la propuesta como la asignación actual, que nos servirá de plan de vuelta atrás:

```bash
cd /planes
kafka-reassign-partitions --bootstrap-server kafka1:29092 \
  --topics-to-move-json-file topics-a-mover.json --broker-list "1,2,3,4" --generate > salida-generate.txt
cat salida-generate.txt
sed -n '/Proposed partition reassignment configuration/{n;p}' salida-generate.txt > plan.json
sed -n '/Current partition replica assignment/{n;p}' salida-generate.txt > rollback.json
```

Ejecutamos con un límite de 10 MB/s para el tráfico de replicación:

```bash
kafka-reassign-partitions --bootstrap-server kafka1:29092 \
  --reassignment-json-file plan.json --execute --throttle 10485760
kafka-reassign-partitions --bootstrap-server kafka1:29092 \
  --reassignment-json-file plan.json --verify
```

Mientras dura la reasignación, describe el topic y verás particiones con más réplicas de las que tocan (las antiguas más las nuevas), y también las propiedades de throttle aplicadas:

```bash
kafka-configs --bootstrap-server kafka1:29092 --describe --entity-type brokers --entity-name 1 | grep -i throttle
kafka-configs --bootstrap-server kafka1:29092 --describe --entity-type topics --entity-name flota.posiciones | grep -i throttled
```

Repite `--verify` hasta que todas las particiones digan que la reasignación se ha completado. Esto es importante: es `--verify` quien elimina las propiedades de throttle cuando todo ha terminado. Si te olvidas de verificar, el límite de replicación se queda puesto y te lo encontrarás meses después como un misterioso problema de réplicas que no alcanzan al líder.

## Paso 3 · Subir el factor de replicación

No existe un `--alter --replication-factor`; el factor de replicación se cambia con una reasignación cuyo JSON indica explícitamente más réplicas por partición. Tienes el fichero preparado en `subir-rf-a-3.json`:

```bash
cat subir-rf-a-3.json
kafka-reassign-partitions --bootstrap-server kafka1:29092 \
  --reassignment-json-file subir-rf-a-3.json --execute --throttle 10485760
kafka-reassign-partitions --bootstrap-server kafka1:29092 \
  --reassignment-json-file subir-rf-a-3.json --verify
kafka-topics --bootstrap-server kafka1:29092 --describe --topic flota.posiciones
```

Ahora que el topic tiene tres réplicas, ya tiene sentido fijarle `min.insync.replicas=2`:

```bash
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type topics \
  --entity-name flota.posiciones --add-config min.insync.replicas=2
```

## Paso 4 · Líder preferido

Después de mover réplicas, los líderes suelen quedar desequilibrados. Fuerza una elección de líder preferido y comprueba que cada partición vuelve a estar liderada por la primera réplica de su lista:

```bash
kafka-leader-election --bootstrap-server kafka1:29092 --election-type PREFERRED --all-topic-partitions
kafka-topics --bootstrap-server kafka1:29092 --describe --topic flota.posiciones
```

## Paso 5 · Añadir particiones

```bash
kafka-topics --bootstrap-server kafka1:29092 --alter --topic flota.posiciones --partitions 8
kafka-topics --bootstrap-server kafka1:29092 --alter --topic flota.posiciones --partitions 4
```

El primer comando funciona y el segundo falla: en Kafka las particiones solo pueden aumentar. Reflexiona sobre la consecuencia del primero: a partir de ahora el particionador por defecto calcula `hash(clave) % 8` en lugar de `% 6`, así que un mismo vehículo puede empezar a escribir en una partición distinta y se rompe el orden por clave entre los datos antiguos y los nuevos.

## Paso 6 · Configuración dinámica y precedencia

Kafka distingue configuraciones de solo lectura (requieren reinicio), por broker (`per-broker`) y de clúster (`cluster-wide`). Vamos a probar las tres situaciones.

```bash
# Valor por defecto para todo el clúster (cluster-wide, dinámico)
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type brokers --entity-default \
  --add-config log.retention.ms=259200000

# Valor específico para el broker 1 (per-broker, dinámico)
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type brokers --entity-name 1 \
  --add-config log.retention.ms=172800000

# Valor a nivel de topic (gana siempre a los de broker para ese topic)
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type topics --entity-name flota.posiciones \
  --add-config retention.ms=86400000

# Ver los "synonyms": el orden en el que Kafka resuelve el valor
kafka-configs --bootstrap-server kafka1:29092 --describe --entity-type brokers --entity-name 1 --all \
  | grep "log.retention.ms"
```

En la línea de `synonyms` verás el orden real de resolución: `DYNAMIC_BROKER_CONFIG` (el del broker concreto), después `DYNAMIC_DEFAULT_BROKER_CONFIG` (el valor por defecto del clúster), después `STATIC_BROKER_CONFIG` (lo que hay en `server.properties`) y por último `DEFAULT_CONFIG` (el valor por defecto de Kafka). Y para un topic concreto, la configuración del topic se impone a todas ellas.

Intenta ahora cambiar en caliente algo que no es dinámico:

```bash
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type brokers --entity-name 1 \
  --add-config log.dirs=/tmp/otro
```

El error te recuerda que esa propiedad exige un reinicio, que en producción se hace con un *rolling restart* broker a broker, esperando a que no haya particiones under-replicated antes de pasar al siguiente.

Deja las cosas como estaban:

```bash
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type brokers --entity-name 1 --delete-config log.retention.ms
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type brokers --entity-default --delete-config log.retention.ms
```

## Paso 7 (demostración opcional) · Self-Balancing Clusters

Confluent Server incluye Self-Balancing Clusters (SBC), que hace automáticamente lo que acabamos de hacer a mano. Lo activamos de forma dinámica para todo el clúster y le pedimos que reequilibre ante cualquier desigualdad de carga:

```bash
kafka-configs --bootstrap-server kafka1:29092 --alter --entity-type brokers --entity-default \
  --add-config confluent.balancer.enable=true,confluent.balancer.heal.uneven.load.trigger=ANY_UNEVEN_LOAD
kafka-rebalance-cluster --bootstrap-server kafka1:29092 --status
```

Para ver SBC drenando un broker, solicita la retirada del broker 4 y sigue su estado:

```bash
kafka-remove-brokers --bootstrap-server kafka1:29092 --broker-id 4 --delete
kafka-remove-brokers --bootstrap-server kafka1:29092 --broker-id 4 --describe
```

SBC necesita unos minutos para recopilar métricas antes de actuar, así que es normal que al principio informe de que todavía no tiene datos suficientes. Si el tiempo de la sesión no da para esperar, el formador lo mostrará al final de la clase. Ten en cuenta que esta función es de Confluent Platform (licencia Enterprise, con 30 días de prueba) y que en Apache Kafka la alternativa automática habitual es Cruise Control.

## Preguntas de comprobación

1. ¿Qué tres fases tiene `kafka-reassign-partitions` y cuál de ellas elimina el throttle?
2. ¿Cómo se cambia el factor de replicación de un topic existente?
3. ¿Por qué añadir particiones a un topic con clave puede romper el orden de los eventos?
4. Si un topic tiene `retention.ms` definido y el broker tiene `log.retention.hours`, ¿cuál se aplica?
5. ¿Qué diferencia hay entre una configuración `per-broker` y una `cluster-wide`?

## Limpieza

```bash
docker compose --profile escalado down -v
```

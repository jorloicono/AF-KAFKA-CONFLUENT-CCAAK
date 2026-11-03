# Lab 10 · Replicar un topic entre dos clústeres con Cluster Linking, hacer failover y compararlo con MirrorMaker 2

**Bloque 8 · Duración estimada: 75 minutos**

## Objetivo

La replicación dentro de un clúster protege frente a la caída de un broker, pero no frente a la pérdida de un centro de datos o de una región entera. Para eso necesitamos copiar datos entre clústeres. En este laboratorio montamos dos clústeres, `madrid` (producción) y `valencia` (recuperación ante desastres), replicamos un topic con Cluster Linking, simulamos la pérdida de Madrid, hacemos failover y movemos un consumidor a Valencia sin perder su posición. Después replicamos otro topic con MirrorMaker 2 y comparamos ambas herramientas en las dimensiones que pregunta el examen: nombres de topic, offsets, infraestructura necesaria y sincronización de grupos.

## Punto de partida

```bash
cd labs/lab10-cluster-linking-mm2
docker compose up -d
sleep 20
```

Crea los topics en Madrid y carga datos. Usaremos dos terminales: una en `madrid` y otra en `valencia`.

```bash
docker exec -it madrid bash
kafka-topics --bootstrap-server madrid:29092 --create --topic flota.posiciones --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server madrid:29092 --create --topic flota.vehiculos --partitions 3 --replication-factor 1 --config cleanup.policy=compact
bash /scripts/generar-flota.sh 3000 madrid:29092
bash /scripts/cargar-vehiculos.sh madrid:29092
```

Simula una aplicación que ya ha procesado parte de los datos en Madrid, leyendo 1.000 mensajes con el grupo `app-rutas`:

```bash
kafka-console-consumer --bootstrap-server madrid:29092 --topic flota.posiciones --group app-rutas \
  --from-beginning --max-messages 1000 > /dev/null
kafka-consumer-groups --bootstrap-server madrid:29092 --describe --group app-rutas
```

## Paso 1 · Crear el cluster link

El enlace se crea siempre en el clúster de destino, que es el que tira de los datos del origen. En la terminal de Valencia:

```bash
docker exec -it valencia bash
cat /lab/link.properties
kafka-cluster-links --bootstrap-server valencia:29092 --create --link madrid-a-valencia \
  --config-file /lab/link.properties
kafka-cluster-links --bootstrap-server valencia:29092 --list
```

## Paso 2 · Crear el mirror topic

```bash
kafka-mirrors --create --mirror-topic flota.posiciones --link madrid-a-valencia \
  --bootstrap-server valencia:29092
kafka-mirrors --describe --topics flota.posiciones --bootstrap-server valencia:29092
kafka-topics --bootstrap-server valencia:29092 --describe --topic flota.posiciones
```

El mirror topic tiene el mismo nombre y el mismo número de particiones que el original. Compara ahora los offsets finales de cada partición en los dos clústeres:

```bash
kafka-get-offsets --bootstrap-server madrid:29092 --topic flota.posiciones
kafka-get-offsets --bootstrap-server valencia:29092 --topic flota.posiciones
```

Son idénticos: Cluster Linking replica los datos byte a byte y conserva los offsets, igual que hace un follower dentro de un clúster. Intenta escribir en el mirror topic y comprueba que está en solo lectura:

```bash
echo 'V001|{"vehiculo_id":"V001"}' | kafka-console-producer --bootstrap-server valencia:29092 \
  --topic flota.posiciones --property parse.key=true --property key.separator='|'
```

## Paso 3 · Sincronización de grupos de consumidores

Pasados unos segundos (`consumer.offset.sync.ms=5000`), el grupo `app-rutas` aparece en Valencia con los mismos offsets confirmados que en Madrid:

```bash
kafka-consumer-groups --bootstrap-server valencia:29092 --describe --group app-rutas
```

Genera más datos en Madrid desde la otra terminal (`bash /scripts/generar-flota.sh 2000 madrid:29092`) y comprueba que el mirror topic los recibe en pocos segundos.

## Paso 4 · Desastre en Madrid y failover

Desde tu equipo, simula la pérdida del centro de Madrid:

```bash
docker stop madrid
```

En Valencia, convierte el mirror topic en un topic normal y escribible. Existen dos operaciones: `--promote` espera a que el mirror esté completamente sincronizado con el origen (requiere que el origen responda) y `--failover` lo hace inmediatamente, aceptando que lo que no se haya replicado se pierda. Con Madrid caído, la opción es `--failover`:

```bash
kafka-mirrors --failover --topics flota.posiciones --bootstrap-server valencia:29092
kafka-mirrors --describe --topics flota.posiciones --bootstrap-server valencia:29092
```

Ahora la aplicación se reconecta a Valencia y continúa donde lo dejó, sin reprocesar los 1.000 primeros mensajes:

```bash
kafka-console-consumer --bootstrap-server valencia:29092 --topic flota.posiciones --group app-rutas \
  --max-messages 5 --property print.offset=true --property print.partition=true
bash /scripts/generar-flota.sh 100 valencia:29092
```

El RPO de esta arquitectura es el retraso de replicación del enlace en el momento del desastre (lo que no había llegado a Valencia se pierde) y el RTO es lo que tardes en hacer failover y redirigir a los clientes. Por eso en producción el failover se automatiza y los clientes se configuran con un nombre DNS que se puede cambiar.

## Paso 5 · MirrorMaker 2

Vuelve a arrancar Madrid y lanza MirrorMaker 2 para replicar el maestro de vehículos:

```bash
docker start madrid
sleep 15
docker compose --profile mm2 up -d mm2
sleep 30
docker exec valencia kafka-topics --bootstrap-server valencia:29092 --list
```

Observa los topics que ha creado MM2 en Valencia: el topic replicado se llama `madrid.flota.vehiculos` (la política de replicación por defecto antepone el alias del clúster de origen), y aparecen topics auxiliares como `heartbeats`, `madrid.checkpoints.internal` y los topics internos de Connect de MM2. Consume el topic replicado:

```bash
docker exec valencia kafka-console-consumer --bootstrap-server valencia:29092 \
  --topic madrid.flota.vehiculos --from-beginning --max-messages 5 --property print.key=true
```

## Paso 6 · Comparación

Con lo que has visto, completa esta tabla y coméntala con el resto de la clase:

| Aspecto | Cluster Linking | MirrorMaker 2 |
|---|---|---|
| Dónde se ejecuta | | |
| Nombre del topic en destino | | |
| Offsets en destino | | |
| Sincronización de grupos | | |
| ¿El topic destino admite escrituras? | | |
| Infraestructura adicional | | |
| Disponibilidad | | |

La respuesta esperada, en resumen, es que Cluster Linking vive dentro de los brokers del destino (sin infraestructura extra), mantiene el nombre y los offsets idénticos, sincroniza grupos y ACLs, y deja el mirror en solo lectura hasta el failover o la promoción, pero requiere Confluent Server o Confluent Cloud en el destino. MirrorMaker 2 es open source y funciona entre cualquier pareja de clústeres Kafka, pero se ejecuta sobre Kafka Connect (más piezas que operar), renombra los topics por defecto y los offsets no tienen por qué coincidir, por lo que necesita traducirlos con los checkpoints. Confluent Replicator, la tercera opción, también es un conector de Connect y es la herramienta heredada de Confluent para este caso de uso.

## Preguntas de comprobación

1. ¿En qué clúster se crea el cluster link y por qué?
2. ¿Qué diferencia hay entre `kafka-mirrors --promote` y `--failover`?
3. ¿Por qué el consumidor no reprocesa mensajes al pasar a Valencia?
4. ¿Cómo se llama por defecto en destino un topic `pedidos` replicado por MM2 desde un clúster con alias `primario`?
5. ¿Qué determina el RPO de una arquitectura de DR con Cluster Linking?

## Limpieza

```bash
docker compose --profile mm2 down -v
```

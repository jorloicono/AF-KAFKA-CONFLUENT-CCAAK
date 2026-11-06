# Bloque 7 · Incidencias y rendimiento

**Sesión 8 · viernes 13 de noviembre** · Presentación: [`presentaciones/Bloque_07_Incidencias_y_rendimiento.pptx`](../presentaciones/Bloque_07_Incidencias_y_rendimiento.pptx) · Laboratorio: [Lab 8](../labs/lab08-simulacro-incidencias/README.md)

El bloque combina un simulacro de guardia, con cinco incidencias provocadas, y el diseño que las previene: dimensionamiento, retención y compactación, y ajuste del clúster.

## 1. Simulacro de incidencias (Lab 8)

Se trabaja sobre el entorno del Lab 7 (tres brokers, Prometheus y Grafana), con el broker 3 sobre un disco de 400 MB. Para cada incidencia hay 12 minutos para rellenar una hoja de diagnóstico:

| Campo | Contenido |
|---|---|
| Síntoma | Lo que diría el usuario |
| Evidencias | Grafana y alertas, CLI, logs |
| Causa raíz | Qué ha pasado realmente |
| Impacto | ¿Se pierden datos? ¿Quién está afectado? |
| Acción correctora | Qué hacer |
| Validación | Cómo saber que está resuelto |
| Prevención | Qué cambiar en configuración o monitorización |

Conviene separar lo que se observa ("el panel marca una partición offline") de lo que se deduce ("el broker 3 está caído"). La mayoría de incidencias se previenen en el diseño: replicación insuficiente, retención sin límite, consumidores sin vigilancia o clientes inestables.

## 2. Dimensionamiento de brokers y particiones

**Disco:**

`caudal de entrada × segundos de retención × factor de replicación × margen (1,3–1,4)`

Ejemplo: 50 MB/s × 7 días × RF 3 ≈ 90 TB; con margen, unos 120–130 TB. La compresión en el productor reduce esta cifra (se almacena comprimido). **Tiered Storage** permite mover los segmentos antiguos a almacenamiento de objetos y dejar en los brokers solo los datos recientes.

**Red** (a nivel de clúster):

- Entrada de productores: E.
- Replicación entre brokers: E × (RF − 1).
- Salida a consumidores: E × número de grupos que leen todo.

Con 50 MB/s, RF 3 y tres grupos: 50 + 100 + 150 ≈ 300 MB/s.

**Particiones de un topic:** máximo entre caudal objetivo ÷ caudal por partición del productor y caudal objetivo ÷ caudal por partición del consumidor, medidos con pruebas de carga, más margen. Si el topic tiene clave, conviene sobredimensionar desde el principio, porque aumentar particiones rompe el orden por clave.

Las particiones no son gratis: cada réplica consume ficheros abiertos, memoria y trabajo de replicación; muchas particiones alargan la recuperación tras la caída de un broker y reducen el tamaño de los lotes. KRaft ha elevado el límite práctico, y Confluent Cloud publica límites por tipo de clúster.

**Recursos del broker:** heap de Java moderado (unos 6 GB); el resto de la RAM la aprovecha el sistema operativo como **page cache**, que sirve los datos recientes sin tocar disco. CPU sobre todo para TLS, compresión y número de peticiones. Discos dedicados, XFS, en JBOD o RAID.

## 3. Retención y compactación

Cada partición se divide en **segmentos**. Kafka escribe en el **segmento activo** y lo cierra al alcanzar `segment.bytes` (1 GB) o `segment.ms` (7 días). Retención y compactación solo actúan sobre segmentos cerrados.

| | `cleanup.policy=delete` (por defecto) | `cleanup.policy=compact` |
|---|---|---|
| Qué conserva | Mensajes dentro de la retención | Al menos el último valor de cada clave |
| Cuándo actúa | Segmentos cerrados más antiguos que `retention.ms` o por encima de `retention.bytes` (por partición), revisado cada `log.retention.check.interval.ms` | Cuando la parte no compactada supera `min.cleanable.dirty.ratio` (0,5) |
| Offsets | Se mantienen | Se mantienen, con huecos |
| Uso típico | Flujos de eventos | Estado actual por clave; topics internos |

Consecuencias:

- La retención es aproximada y por exceso. Un topic con poco tráfico puede conservar datos mucho más tiempo del configurado si su segmento activo no se cierra; para borrar a tiempo hay que ajustar `segment.ms`.
- Un mensaje con valor nulo (**lápida**) borra una clave en un topic compactado. La lápida se conserva `delete.retention.ms` (24 h) para que los consumidores rezagados vean el borrado.
- `min.compaction.lag.ms` y `max.compaction.lag.ms` fijan el tiempo mínimo y máximo antes de compactar.
- `compact,delete` compacta y además borra por retención.
- Si el *log cleaner* falla (por ejemplo, por un mensaje sin clave en un topic compactado), las particiones compactadas crecen sin control; conviene vigilar el tiempo desde su última ejecución.

## 4. Tuning del clúster

Orden recomendado: primero clientes, después broker, después sistema operativo.

| Ámbito | Parámetros | Efecto |
|---|---|---|
| Productor | `batch.size`, `linger.ms` (0 hasta Kafka 3.9; 5 ms en 4.0) | Lotes más grandes: más rendimiento y mejor compresión, algo más de latencia |
| Productor | `compression.type` (lz4, zstd, snappy, gzip) | Menos red y disco a cambio de CPU en el productor |
| Productor | `acks` | Durabilidad frente a latencia |
| Consumidor | `fetch.min.bytes`, `fetch.max.wait.ms`, `max.poll.records` | Menos peticiones a cambio de latencia; tamaño de cada `poll` |
| Broker | `num.network.threads` (3) | Subir si `NetworkProcessorAvgIdlePercent` es bajo |
| Broker | `num.io.threads` (8) | Subir si `RequestHandlerAvgIdlePercent` es bajo y crece `RequestQueueTimeMs` |
| Broker | `num.replica.fetchers` (1) | Subir si los followers no siguen el ritmo y el ISR oscila |
| Topic | `compression.type=producer` | Evita que el broker recomprima los lotes |
| Sistema | Descriptores de fichero altos, `vm.max_map_count` alto, swap casi desactivado, `noatime`; G1 GC | Estabilidad |

**Cuotas:** por usuario, `client.id` o ambos, de bytes producidos, bytes consumidos y porcentaje de tiempo de los hilos de petición. Se configuran en caliente con `kafka-configs`. Al superarlas, el broker no devuelve errores: retrasa las respuestas (**throttling**).

## Lo que pregunta el examen

- Cálculo de disco a partir de caudal, retención y RF; tráfico de replicación.
- Estimación de particiones; costes de demasiadas particiones; papel de la page cache.
- Se borran segmentos cerrados y nunca el activo; `retention.bytes` por partición; efecto de `segment.ms`.
- Compactación: último valor por clave, offsets conservados, lápidas, `delete.retention.ms`, `min.cleanable.dirty.ratio`, `compact,delete`.
- Efecto de `batch.size`, `linger.ms` y compresión; qué hilo subir según la métrica; `compression.type=producer`; cuotas con throttling.

## Para practicar

- Simulacro del Bloque 7.
- Lab 8: [`labs/lab08-simulacro-incidencias`](../labs/lab08-simulacro-incidencias/README.md).

# Bloque 8 · Rendimiento, alta disponibilidad y recuperación ante desastres

**Sesiones 9 y 10 · lunes 16 y miércoles 18 de noviembre** · Presentación: [`presentaciones/Bloque_08_Rendimiento_alta_disponibilidad_y_DR.pptx`](../presentaciones/Bloque_08_Rendimiento_alta_disponibilidad_y_DR.pptx) · Laboratorios: [Lab 9](../labs/lab09-rendimiento/README.md), [Lab 10](../labs/lab10-cluster-linking-mm2/README.md)

El bloque mide el rendimiento con pruebas de carga, estudia la alta disponibilidad dentro del clúster y entre centros, las estrategias de failover y backup, las herramientas de replicación entre clústeres y la arquitectura de ksqlDB.

## 1. Pruebas de carga (Lab 9)

Kafka incluye `kafka-producer-perf-test` (registros/s, MB/s, latencia media, máxima y percentiles) y `kafka-consumer-perf-test`. Con `--throughput -1` no se limita el ritmo; con un valor fijo se mide la latencia a carga constante.

Tendencias que suelen observarse (los números absolutos de un portátil no son representativos; importan las diferencias):

- `acks=0` > `acks=1` > `acks=all` en rendimiento; lo contrario en durabilidad.
- Subir `batch.size` y `linger.ms` mejora mucho el rendimiento a cambio de milisegundos de latencia.
- Con JSON, zstd suele comprimir más; lz4 y snappy son más rápidos; gzip comprime bien pero cuesta más CPU.
- Un topic de una partición no aprovecha el paralelismo del clúster.

## 2. Alta disponibilidad

| Mecanismo | Qué hace |
|---|---|
| `broker.rack` | Kafka reparte las réplicas de cada partición entre racks o zonas distintas (al crear topics y al generar reasignaciones; un JSON hecho a mano puede saltárselo) |
| *Follower fetching* | El consumidor declara `client.rack` y el broker usa el selector de réplicas consciente de racks (`replica.selector.class`) para leer de la réplica de su zona: menos latencia y menos coste entre zonas |
| *Stretched cluster* | Un clúster repartido entre zonas o centros con replicación síncrona. Entre tres zonas de una región es lo estándar; entre dos centros no hay quórum que sobreviva a la caída de cualquiera de ellos sin un tercer emplazamiento de desempate ("2,5 centros") |
| Multi-Region Clusters (Confluent) | **Observers**: réplicas asíncronas fuera del ISR que no añaden latencia a `acks=all` y se pueden promover si cae la región principal, según restricciones de colocación por topic |
| Dos clústeres replicados | Activo-pasivo (se escribe en uno; el otro es copia) o activo-activo (se escribe en ambos; hay que evitar bucles y no hay orden global) |

Ejemplo: seis brokers en tres zonas, `broker.rack` configurado, RF 3 y `min.insync.replicas=2`. Si cae una zona, quedan dos réplicas por partición y las escrituras con `acks=all` continúan.

## 3. Failover, backup y recuperación

| | Definición | Con replicación síncrona entre zonas | Con replicación asíncrona entre clústeres |
|---|---|---|---|
| **RPO** | Cuántos datos se acepta perder | Cero | El retraso de la replicación en el momento del desastre |
| **RTO** | Cuánto se tarda en volver a dar servicio | Segundos | Lo que se tarde en hacer failover y redirigir clientes |

Un failover tiene tres partes:

1. **Datos:** los topics en el secundario, idealmente con los mismos offsets.
2. **Offsets de consumidores:** sincronizados o traducidos, para no reprocesar ni perder.
3. **Clientes:** redirigidos, normalmente con un DNS o configuración centralizada.

El *failback* también se planifica: el primario vuelve desfasado.

**Backups.** La replicación protege frente al hardware, no frente al error humano o la corrupción lógica (un topic borrado o datos erróneos también se replican). Estrategias:

- Volcar los topics críticos a almacenamiento de objetos con un conector sink, y restaurar con el source correspondiente.
- Versionar como código la configuración: topics, ACLs, role bindings, conectores, esquemas.
- Proteger especialmente `_schemas`.

Tiered Storage abarata la retención larga, pero no es un backup: al borrar el topic se borran también sus segmentos remotos.

## 4. Replicación entre clústeres

| | MirrorMaker 2 | Confluent Replicator | Cluster Linking |
|---|---|---|---|
| Licencia | Abierto (Apache Kafka) | Comercial | Confluent Server o Confluent Cloud en el destino; origen puede ser Apache Kafka |
| Dónde se ejecuta | Sobre Kafka Connect (o modo dedicado) | Conector de Connect | Dentro de los brokers del destino |
| Nombre en destino | `<alias-origen>.<topic>` por defecto (*remote topic*) | Configurable | Igual que el original |
| Offsets | Pueden diferir; se traducen con *checkpoints* | Traducción con interceptor por marcas de tiempo | Idénticos, byte a byte |
| Sincronización de grupos | Sí, con offsets traducidos | Con interceptores | Sí, directa |
| Topic en destino | Escribible | Escribible | *Mirror topic* de solo lectura hasta `promote` o `failover` |

En Cluster Linking, el enlace se crea **en el destino**. `kafka-mirrors --promote` espera a que el mirror esté sincronizado (necesita el origen disponible); `--failover` lo convierte inmediatamente, aceptando perder lo no replicado. Existe un modo en que el enlace lo inicia el origen, útil cuando el origen está tras un cortafuegos. *Schema Linking* replica esquemas.

Regla práctica: destino en Confluent Server o Cloud → Cluster Linking; ambos extremos Apache Kafka → MirrorMaker 2; Replicator en instalaciones que ya lo usan.

## 5. ksqlDB: arquitectura y administración

ksqlDB es un servidor que traduce SQL a aplicaciones de **Kafka Streams**.

- **Stream:** secuencia inmutable de eventos. **Table:** último estado por clave, normalmente sobre un topic compactado.
- **Consulta persistente:** `CREATE STREAM/TABLE ... AS SELECT`; corre de forma continua en el servidor y escribe en un topic.
- **Estado:** almacenes RocksDB locales respaldados por topics **changelog**; los cambios de clave generan topics de **repartición**. Todos llevan un prefijo derivado de `ksql.service.id`.
- **Clúster:** los servidores con el mismo `ksql.service.id` se reparten el trabajo; para escalar se añaden servidores. Las réplicas en espera reducen el tiempo de recuperación.
- **Modo interactivo:** acepta sentencias por API REST y las guarda en el **command topic**, fuente de verdad para reconstruir las consultas al reiniciar. **Modo headless:** las consultas vienen de `ksql.queries.file` y no hay command topic.
- **Operación:** `SHOW QUERIES`, `EXPLAIN`, `TERMINATE`, endpoints `/info` y `/healthcheck`, métricas JMX y el **processing log**, donde quedan los errores de deserialización.
- Confluent mantiene ksqlDB, pero su apuesta estratégica es Apache Flink.

## Lo que pregunta el examen

- `broker.rack` y follower fetching; limitaciones de dos centros; observers.
- RPO y RTO; partes de un failover; la replicación no sustituye a un backup.
- Diferencias entre MM2, Replicator y Cluster Linking; nombres, offsets, mirror topics, `promote` frente a `failover`.
- Kafka Streams bajo ksqlDB; stream y tabla; command topic, changelog y repartición; interactivo frente a headless.

## Para practicar

- Simulacro del Bloque 8.
- Labs [9](../labs/lab09-rendimiento/README.md) y [10](../labs/lab10-cluster-linking-mm2/README.md).

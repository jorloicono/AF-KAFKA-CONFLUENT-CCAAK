# Bloque 2 · Clúster en marcha y configuración

**Sesión 2 · miércoles 4 de noviembre** · Presentación: [`presentaciones/Bloque_02_Cluster_en_marcha_y_configuracion.pptx`](../presentaciones/Bloque_02_Cluster_en_marcha_y_configuracion.pptx) · Laboratorio: [Lab 1](../labs/lab01-cluster-kraft/README.md)

Este bloque pasa de la teoría a un clúster real y cubre el dominio con más peso del examen: la configuración del clúster. Se ven las opciones de despliegue, cómo se configura un broker, en qué orden se aplican las configuraciones y cómo se gestiona un topic durante su vida.

## 1. Lab 1 · Clúster KRaft de 3 brokers

El Lab 1 levanta tres nodos en modo combinado (broker y controller) y recorre las herramientas que más aparecen en el examen:

| Herramienta | Para qué |
|---|---|
| `kafka-metadata-quorum describe --status / --replication` | Líder del quórum, época, votantes y retraso de cada nodo |
| `kafka-topics --create / --describe / --list` | Crear y describir topics: líder, réplicas e ISR por partición |
| `kafka-configs --describe --all` | Configuración efectiva de un topic o un broker |
| `kafka-console-producer / -consumer` | Producir y consumir, con clave, partición y offset |
| `kafka-consumer-groups --describe` | Offsets confirmados, final del log y lag por partición |
| `kafka-log-dirs`, `kafka-dump-log` | Ocupación en disco y contenido de los segmentos |
| `kafka-leader-election --election-type PREFERRED` | Devolver el liderazgo a la réplica preferida |

Al detener un broker se observa que sus particiones cambian de líder, que el broker desaparece del **ISR** pero no de la lista de **Replicas** (que es la asignación) y que, con RF 3 y `min.insync.replicas=2`, se sigue escribiendo con `acks=all`. Con dos brokers caídos se pierden a la vez el quórum y la capacidad de escribir.

En los contenedores de Confluent, cada variable `KAFKA_ALGO_ASI` del `docker-compose.yml` se convierte en la propiedad `algo.asi` del fichero de configuración del broker, que se genera al arrancar en `/etc/kafka/kafka.properties`.

## 2. Despliegue: Confluent Platform frente a Confluent Cloud

**Confluent Platform** es la distribución autogestionada. Sus componentes:

| Componente | Función |
|---|---|
| Confluent Server | Kafka con funciones Enterprise: RBAC, Self-Balancing Clusters, Cluster Linking, Tiered Storage, logs de auditoría, validación de esquemas en el broker |
| Schema Registry | Gestión y compatibilidad de esquemas |
| Kafka Connect y Confluent Hub | Integración con sistemas externos mediante conectores |
| ksqlDB y Confluent Platform for Apache Flink | Procesamiento de streams |
| REST Proxy | Producir y consumir por HTTP |
| Control Center | Consola web de gestión y monitorización |

Las funciones Enterprise necesitan licencia; sin ella funcionan 30 días de prueba. Vías de instalación: paquetes o ficheros comprimidos, **Ansible** (playbooks oficiales), imágenes **Docker** y **Confluent for Kubernetes** (operador declarativo).

**Confluent Cloud** es el servicio gestionado. Se organiza en organización, **entornos** (habitualmente dev, pre y pro) y clústeres. Tipos de clúster, de menos a más aislamiento y garantías: *Basic* (desarrollo), *Standard* (producción general), *Enterprise* (red privada y escalado elástico), *Dedicated* (capacidad reservada en CKU) y *Freight* (grandes volúmenes con latencia relajada).

| | Lo gestiona el proveedor | Lo gestiona el cliente |
|---|---|---|
| Confluent Cloud | Brokers, controllers, sistema operativo, parches, capacidad, reequilibrado | Topics, cuentas de servicio, permisos, API keys, esquemas, conectores, red de acceso |
| Confluent Platform | — | Todo, con acceso a cualquier parámetro |

## 3. Configuración de brokers

Propiedades principales, agrupadas por función:

| Grupo | Propiedades |
|---|---|
| Identidad y roles | `node.id` (antes `broker.id`), `process.roles`, `controller.quorum.voters` / `controller.quorum.bootstrap.servers` |
| Red | `listeners`, `advertised.listeners`, `listener.security.protocol.map`, `inter.broker.listener.name`, `controller.listener.names` |
| Almacenamiento | `log.dirs` (uno o varios directorios, JBOD) |
| Valores por defecto de topics | `num.partitions`, `default.replication.factor`, `min.insync.replicas`, `log.retention.*`, `log.segment.bytes`, `auto.create.topics.enable` |
| Replicación | `replica.lag.time.max.ms`, `unclean.leader.election.enable`, `num.replica.fetchers` |
| Hilos | `num.network.threads`, `num.io.threads` |
| Topics internos | `offsets.topic.replication.factor`, `transaction.state.log.replication.factor` (3 en producción) |

`advertised.listeners` es la dirección que el broker anuncia a los clientes. Si no es alcanzable desde ellos (un nombre interno de Docker, una IP privada), el cliente conecta al bootstrap pero falla al conectar con el broker. En producción conviene `auto.create.topics.enable=false`.

**Modos de actualización.** Cada propiedad tiene un *Update Mode* en la documentación:

| Modo | Se cambia | Ejemplos |
|---|---|---|
| `read-only` | Solo reiniciando el broker | `node.id`, `log.dirs`, `process.roles` |
| `per-broker` | En caliente, para un broker concreto | Listeners, certificados |
| `cluster-wide` | En caliente, para todo el clúster o un broker | `log.retention.ms`, `num.io.threads` |

Los cambios dinámicos se hacen con `kafka-configs` (`--entity-name N` para un broker, `--entity-default` para todo el clúster) y se guardan en los metadatos del clúster, no en el fichero.

**Precedencia**, de mayor a menor:

1. Configuración del **topic** (por ejemplo `retention.ms`).
2. Configuración dinámica del **broker concreto** (`DYNAMIC_BROKER_CONFIG`).
3. Configuración dinámica **por defecto del clúster** (`DYNAMIC_DEFAULT_BROKER_CONFIG`).
4. Configuración **estática** del fichero (`STATIC_BROKER_CONFIG`).
5. Valor **por defecto** de Kafka (`DEFAULT_CONFIG`).

`kafka-configs --describe --all` muestra este orden en la línea `synonyms`. En el broker, las propiedades por defecto de los topics llevan el prefijo `log.` (`log.retention.ms`, `log.cleanup.policy`); en el topic se escriben sin él (`retention.ms`, `cleanup.policy`). Excepción: `message.max.bytes` en el broker equivale a `max.message.bytes` en el topic.

**Rolling restart.** Los cambios de solo lectura se aplican reiniciando un broker cada vez y esperando a que no haya particiones under-replicated antes de seguir. El apagado controlado traspasa el liderazgo antes de parar. En KRaft, al actualizar de versión: primero controllers, después brokers y, al final, subir la versión de metadatos con `kafka-features`. Con ZooKeeper se hacía en dos pasadas con `inter.broker.protocol.version`.

## 4. Gestión de topics

| Operación | Cómo |
|---|---|
| Crear | `kafka-topics --create --partitions N --replication-factor R --config clave=valor`, o con `--replica-assignment` para fijar la ubicación de cada réplica |
| Aumentar particiones | `kafka-topics --alter --partitions N` (solo aumentar; cambia el reparto por clave) |
| Mover réplicas | `kafka-reassign-partitions` en tres fases: generar, ejecutar (con throttle) y verificar |
| Cambiar el factor de replicación | Reasignación con un JSON que asigna más réplicas por partición |
| Cambiar configuración | `kafka-configs --alter --entity-type topics --add-config ...` |
| Borrar | `kafka-topics --delete` (requiere `delete.topic.enable`, activo por defecto) |

Configuraciones de topic habituales: `min.insync.replicas`, `retention.ms`, `retention.bytes` (**por partición**), `cleanup.policy` (`delete`, `compact` o ambas), `segment.bytes`, `max.message.bytes`.

## Lo que pregunta el examen

- Qué componente aporta cada función (RBAC y Cluster Linking son de Confluent Server).
- Qué gestiona el proveedor y qué el cliente en un servicio gestionado.
- Modos de actualización y orden de precedencia de configuraciones.
- `advertised.listeners` como causa de problemas de conexión.
- Rolling restart esperando a URP = 0.
- Particiones solo aumentan; el RF se cambia con reasignación; `retention.bytes` es por partición; la configuración del topic gana a la del broker.

## Para practicar

- Simulacro del Bloque 2.
- Lab 1: [`labs/lab01-cluster-kraft`](../labs/lab01-cluster-kraft/README.md).

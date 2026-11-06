# Bloque 9 · ksqlDB, Flink e infraestructura como código

**Sesiones 11 y 12 · jueves 19 y viernes 20 de noviembre** · Presentación: [`presentaciones/Bloque_09_ksqlDB_Flink_e_infraestructura_como_codigo.pptx`](../presentaciones/Bloque_09_ksqlDB_Flink_e_infraestructura_como_codigo.pptx) · Laboratorios: [Lab 11](../labs/lab11-ksqldb/README.md), [Lab 12](../labs/lab12-flink-sql/README.md)

El bloque pone en marcha ksqlDB y Apache Flink sobre los datos de la flota, compara ambas herramientas desde el punto de vista del administrador e introduce la infraestructura como código con el provider de Terraform de Confluent.

## 1. ksqlDB en la práctica (Lab 11)

El laboratorio declara un stream sobre `flota.posiciones` y una tabla sobre `flota.vehiculos`, y crea cinco consultas persistentes: un filtro de excesos de velocidad, un enriquecimiento con join stream-tabla, una agregación con ventana *tumbling* de 5 minutos, el estado más reciente por vehículo y un recuento por base.

**Co-particionado.** Un join stream-tabla exige que ambos topics tengan la misma clave, el mismo número de particiones y el mismo particionador. Si no, ksqlDB lo rechaza o hay que repartir uno de ellos.

**Consultas:**

- *Pull queries:* consultan el estado materializado de una tabla y terminan.
- *Push queries* (`EMIT CHANGES`): se suscriben y devuelven resultados según llegan eventos.

**Lo que crea en el clúster** (prefijo `_confluent-ksql-<service.id>`): el *command topic*, los topics `-changelog` de los almacenes de estado y los `-repartition` de los cambios de clave, además de los topics de salida de cada consulta. Ante mensajes que no puede deserializar, ksqlDB no se detiene: los registra en `KSQL_PROCESSING_LOG`.

## 2. Apache Flink

Flink es un motor de procesamiento distribuido independiente, para streaming y lotes, con SQL, Java y Python, y conectores para muchos sistemas además de Kafka.

| Concepto | Descripción |
|---|---|
| JobManager | Coordina: planifica jobs, coordina checkpoints y gestiona la recuperación |
| TaskManager | Ejecuta el trabajo en *slots* |
| Job | Grafo de operadores; cada uno con un grado de paralelismo. Con Kafka, cada instancia de la fuente lee un subconjunto de particiones |
| Estado | Gestionado por Flink, en memoria o en RocksDB (*state backend*) |
| Checkpoint | Instantánea periódica y consistente del estado **y de los offsets de Kafka**, coordinada con barreras y guardada en almacenamiento duradero. Flink se recupera con sus checkpoints, no con los offsets del grupo de consumidores |
| Savepoint | Checkpoint lanzado a mano para parar y retomar un job (cambio de código, paralelismo o versión) |
| Tiempo de evento y watermarks | Las ventanas se calculan sobre el momento en que ocurrió el evento; la watermark indica hasta dónde se han recibido datos. Si una partición no recibe datos, su watermark no avanza (existe un tiempo de inactividad de fuentes) |
| Exactly-once con Kafka | El sink usa transacciones sincronizadas con los checkpoints; los consumidores deben leer con `isolation.level=read_committed` |

**Flink SQL.** Una tabla conecta con un sistema externo:

| Conector | Semántica | Uso |
|---|---|---|
| `kafka` | Flujo de inserciones | Eventos, resultados *append-only* (filtros, ventanas cerradas) |
| `upsert-kafka` (con `PRIMARY KEY`) | Inserciones, actualizaciones y borrados por clave | Topics compactados, resultados que se actualizan (agregaciones sin ventana) |

**Despliegue:** autogestionado (en máquinas o con el operador de Flink para Kubernetes), Confluent Platform for Apache Flink, o Confluent Cloud for Apache Flink, sin servidor, con *compute pools* y los topics y esquemas del entorno disponibles como tablas.

## 3. Flink SQL en la práctica (Lab 12)

El laboratorio levanta un JobManager y un TaskManager, declara tablas sobre los topics de la flota, lanza consultas interactivas y tres jobs persistentes (`INSERT INTO`), comprueba que un job se recupera de la caída de un TaskManager gracias a los checkpoints y lo para con savepoint.

## 4. ksqlDB o Flink

| | ksqlDB | Flink |
|---|---|---|
| Base | Kafka Streams | Motor distribuido propio |
| Fuentes y destinos | Solo Kafka | Kafka y muchos otros sistemas |
| Lenguajes | SQL de ksqlDB | SQL estándar, Java, Python; streaming y lotes |
| Estado | RocksDB + changelog en Kafka | Gestionado, con checkpoints en almacenamiento externo |
| Operación | Sencilla: servidores con el mismo `service.id` | Más exigente si es autogestionado |
| Extra | Pull queries sobre vistas materializadas | Escala a cargas y estados muy grandes |
| Estrategia de Confluent | Mantenido | Apuesta principal (Cloud y Platform) |

Criterio práctico: Flink para proyectos nuevos, sobre todo en Confluent Cloud o con sistemas distintos de Kafka; ksqlDB para casos acotados sobre Kafka o instalaciones existentes; Kafka Streams cuando la lógica vive dentro de una aplicación.

## 5. Infraestructura como código con Terraform

Gestionar la infraestructura como código da revisión, historial, reproducibilidad entre entornos y menos errores manuales.

El provider `confluentinc/confluent` describe recursos de Confluent Cloud (entornos, clústeres, Schema Registry y Stream Governance, cuentas de servicio, API keys, role bindings, topics, ACLs, conectores, compute pools y sentencias de Flink, cluster links, red privada) y algunos de Confluent Platform, como topics y ACLs, a través de su API REST.

| Comando | Función |
|---|---|
| `terraform init` | Descarga los providers |
| `terraform plan` | Muestra qué se crea, modifica o destruye, sin aplicar; es lo que se revisa en una *pull request* |
| `terraform apply` | Aplica los cambios |
| `terraform destroy` | Elimina lo que gestiona la configuración |

El **estado** de Terraform guarda identificadores y secretos (incluidos los de las API keys creadas): nunca se sube al repositorio y en equipos se guarda en un backend remoto cifrado y con bloqueo.

**Credenciales en Confluent Cloud:**

| Tipo | Ámbito | Uso |
|---|---|---|
| Cloud API key | Organización | Gestionar entornos, clústeres, cuentas de servicio |
| API key de recurso (por ejemplo, de clúster) | Un recurso | Producir, consumir, crear topics y ACLs en ese clúster |

Principio de mínimo privilegio: una cuenta de servicio administradora para Terraform y cuentas propias para cada aplicación, con permisos justos.

Para Confluent Platform, los equivalentes declarativos son **Confluent for Kubernetes** y los playbooks de **Ansible**, a menudo dentro de un flujo GitOps.

## Lo que pregunta el examen

- Stream frente a tabla; requisitos del join; consultas persistentes; topics internos de ksqlDB.
- JobManager y TaskManager; checkpoints con offsets; checkpoint frente a savepoint; watermarks; `kafka` frente a `upsert-kafka`; `read_committed` con exactly-once.
- Cuándo elegir ksqlDB o Flink.
- Por qué IaC; `plan` antes de `apply`; estado sensible; Cloud API key frente a API key de recurso; herramientas declarativas por entorno.

## Para practicar

- Simulacro del Bloque 9.
- Labs [11](../labs/lab11-ksqldb/README.md) y [12](../labs/lab12-flink-sql/README.md).

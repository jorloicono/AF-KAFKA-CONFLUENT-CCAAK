# Bloque 5 · Kafka Connect

**Sesiones 5 y 6 · martes 10 y miércoles 11 de noviembre** · Presentación: [`presentaciones/Bloque_05_Kafka_Connect.pptx`](../presentaciones/Bloque_05_Kafka_Connect.pptx) · Laboratorio: [Lab 6](../labs/lab06-connect/README.md)

Kafka Connect es el marco estándar para mover datos entre Kafka y otros sistemas sin escribir código. El bloque cubre su arquitectura, su administración mediante la API REST, el diagnóstico de fallos y, al final, las métricas JMX clave del broker.

## 1. Arquitectura

**Tipos de conector:**

- **Source** (origen): leen de un sistema externo y escriben en Kafka.
- **Sink** (destino): leen de Kafka y escriben en un sistema externo.

**Piezas:**

| Pieza | Qué es |
|---|---|
| Worker | Proceso Java de Kafka Connect que ejecuta el trabajo |
| Connector | Instancia configurada de un plugin; decide cómo dividir el trabajo y configura las tareas |
| Task | Hace la copia real de datos; hasta `tasks.max`, según lo que el conector pueda dividir |
| Converter | Traduce entre la representación interna de Connect y los bytes de Kafka |
| SMT | Transformación ligera mensaje a mensaje |

**Modos:**

| | Standalone | Distribuido |
|---|---|---|
| Workers | Uno | Varios con el mismo `group.id` |
| Configuración de conectores | Ficheros de propiedades al arrancar | API REST |
| Offsets de conectores source | Fichero local (`offset.storage.file.filename`) | Topic de offsets |
| Tolerancia a fallos | No | Sí: las tareas se reparten entre los workers vivos |

**Topics internos del modo distribuido** (compactados, RF 3 en producción):

| Propiedad | Contenido | Particiones habituales |
|---|---|---|
| `config.storage.topic` | Configuración de conectores y tareas | 1 (obligatorio) |
| `offset.storage.topic` | Offsets de los conectores source | 25 |
| `status.storage.topic` | Estado de conectores y tareas | 5 |

Los conectores **sink** son consumidores normales: sus offsets están en `__consumer_offsets` bajo el grupo `connect-<nombre del conector>`, y su lag se ve con `kafka-consumer-groups`.

**Converters:** `AvroConverter`, `ProtobufConverter` y `JsonSchemaConverter` (con Schema Registry), `JsonConverter` (con `schemas.enable=true` incluye esquema y `payload` en cada mensaje), `StringConverter` y `ByteArrayConverter`. Se configuran por separado para clave y valor, en el worker y por conector. El converter de un sink debe coincidir con el formato en que se escribieron los datos.

**SMT:** renombrar o eliminar campos, enrutar a otro topic, extraer una clave de un campo del valor, etc. Se encadenan y pueden aplicarse con predicados. Lo que necesita estado o varios mensajes (agregaciones, joins) corresponde a ksqlDB, Flink o Kafka Streams.

## 2. Despliegue y administración

Propiedades esenciales del worker: `bootstrap.servers`, `group.id`, los tres topics internos y su RF, converters por defecto, `listeners` y `rest.advertised.host.name`, y `plugin.path`.

**Plugins:** se instalan copiándolos en un directorio de `plugin.path` (a mano o con Confluent Hub) en **todos** los workers, y reiniciándolos uno a uno. Cada plugin se carga con su propio cargador de clases aislado. En contenedores, lo habitual es construir una imagen con los conectores ya instalados.

**API REST:**

| Llamada | Función |
|---|---|
| `GET /connectors` | Listar conectores |
| `POST /connectors` | Crear (falla si ya existe) |
| `PUT /connectors/{n}/config` | Crear o actualizar de forma idempotente |
| `GET /connectors/{n}/status` | Estado del conector y de cada tarea, worker y traza del error |
| `PUT .../pause`, `PUT .../resume`, `PUT .../stop` | Pausar, reanudar, detener |
| `POST .../restart?includeTasks=true&onlyFailed=true` | Reiniciar solo lo que ha fallado |
| `DELETE /connectors/{n}` | Borrar |
| `GET /connector-plugins`, `PUT /connector-plugins/{clase}/config/validate` | Plugins instalados y validación de configuración |
| `GET/PUT /admin/loggers` | Cambiar el nivel de log en caliente |

**Escalado:** subir `tasks.max` (limitado por el conector y, en sinks, por el número de particiones) o añadir workers. Connect usa **rebalanceo cooperativo incremental**: cuando un worker desaparece, sus tareas esperan `scheduled.rebalance.max.delay.ms` (5 minutos por defecto) antes de reasignarse, por si vuelve.

**Seguridad:** las credenciales externas no deberían ir en claro en la configuración (que se guarda en un topic y se devuelve por la API); se usan *config providers* (ficheros, variables de entorno, gestores de secretos). `connector.client.config.override.policy` permite que cada conector use sus propias credenciales de cliente.

En Confluent Cloud existen **conectores totalmente gestionados**; para los demás se puede usar un clúster de Connect propio contra Confluent Cloud.

## 3. Troubleshooting de conectores

**Estados:** `UNASSIGNED`, `RUNNING`, `PAUSED`, `STOPPED`, `RESTARTING`, `FAILED`. El estado del conector y el de sus tareas son independientes: un conector `RUNNING` puede tener tareas `FAILED`. Hay que mirar siempre las tareas y su campo `trace` (la causa suele estar en el último *Caused by*), además del log del worker.

Una tarea `FAILED` **no se recupera sola** aunque el sistema externo vuelva: hay que reiniciarla.

**Tipos de error:**

- Del sistema externo (base de datos caída, credenciales caducadas, registro rechazado): el conector reintenta según su configuración y, si no se resuelve, la tarea falla.
- De conversión o transformación (*mensaje envenenado*).
- De configuración (propiedad ausente o inválida, clase no encontrada).

**Tolerancia a errores** (conversión y transformación):

| Propiedad | Efecto |
|---|---|
| `errors.tolerance=none` (por defecto) | El primer error hace fallar la tarea, que queda parada en ese offset |
| `errors.tolerance=all` | Los mensajes erróneos se saltan |
| `errors.log.enable`, `errors.log.include.messages` | Registran el error en el log |
| `errors.deadletterqueue.topic.name` | Envía el mensaje a una DLQ (**solo sinks**) |
| `errors.deadletterqueue.context.headers.enable` | Añade cabeceras `__connect.errors.*` con la causa |

La DLQ solo captura errores de conversión y transformación, no los fallos al escribir en el sistema externo.

**Errores clásicos:**

| Error | Causa |
|---|---|
| *Unknown magic byte* | Converter de Schema Registry leyendo datos que no se escribieron con Schema Registry |
| `JsonConverter` pide `schema` y `payload` | `schemas.enable=true` sobre JSON sin esquema embebido |
| `ClassNotFoundException` al crear el conector | Plugin ausente en algún worker o workers sin reiniciar |
| Sink sin errores que no avanza | Lentitud del destino: se ve en el lag de `connect-<nombre>` |

## 4. Métricas JMX clave del broker

| Métrica (MBean) | Valor sano | Significado |
|---|---|---|
| `ReplicaManager` · `UnderReplicatedPartitions` | 0 | Particiones con réplicas fuera del ISR |
| `ReplicaManager` · `UnderMinIsrPartitionCount` | 0 | Particiones bajo `min.insync.replicas`: fallan las escrituras `acks=all` |
| `ReplicaManager` · `IsrShrinksPerSec` / `IsrExpandsPerSec` | Cerca de 0 | Inestabilidad de réplicas |
| `KafkaController` · `ActiveControllerCount` | Suma de 1 en el clúster | Controlador activo |
| `KafkaController` · `OfflinePartitionsCount` | 0 | Particiones sin líder |
| `UncleanLeaderElectionsPerSec` | 0 | Elecciones con posible pérdida de datos |
| `BrokerTopicMetrics` · `BytesInPerSec`, `BytesOutPerSec`, `MessagesInPerSec` | — | Caudal |
| `BrokerTopicMetrics` · `FailedProduceRequestsPerSec`, `FailedFetchRequestsPerSec` | Cerca de 0 | Peticiones fallidas |
| `KafkaRequestHandlerPool` · `RequestHandlerAvgIdlePercent` | Alto | Ociosidad de los hilos de E/S |
| `SocketServer` · `NetworkProcessorAvgIdlePercent` | Alto | Ociosidad de los hilos de red |

**Descomposición de `RequestMetrics` · `TotalTimeMs`** (por tipo de petición: `Produce`, `FetchConsumer`, `FetchFollower`):

| Componente | Si crece, el cuello de botella está en |
|---|---|
| `RequestQueueTimeMs` | Hilos de E/S saturados |
| `LocalTimeMs` | Disco o procesamiento en el líder |
| `RemoteTimeMs` | Espera a otros brokers (replicación con `acks=all`) |
| `ResponseQueueTimeMs`, `ResponseSendTimeMs` | Hilos de red o red |

Los workers de Connect también publican métricas por JMX: tareas por estado, ritmo de registros y errores enviados a log o DLQ.

## Lo que pregunta el examen

- Worker, connector y task; standalone frente a distribuido; topics internos y su configuración.
- Dónde se guardan los offsets de source y sink.
- Endpoints de la API REST; reinicio con `includeTasks` y `onlyFailed`; plugins en todos los workers.
- Tareas `FAILED` con conector `RUNNING`; `errors.tolerance`; DLQ solo en sinks.
- Nombres y valores normales de las métricas clave; descomposición de `TotalTimeMs`.

## Para practicar

- Simulacro del Bloque 5.
- Lab 6: [`labs/lab06-connect`](../labs/lab06-connect/README.md).

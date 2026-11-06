# Bloque 1 · Eventos y fundamentos

**Sesión 1 · martes 3 de noviembre** · Presentación: [`presentaciones/Bloque_01_Eventos_y_fundamentos.pptx`](../presentaciones/Bloque_01_Eventos_y_fundamentos.pptx)

Este bloque presenta el vocabulario con el que se trabaja el resto del curso: qué es el event streaming, de qué piezas se compone un clúster de Kafka, cómo se organizan y replican los datos y cómo garantizan productores y consumidores la durabilidad y el orden.

## 1. El curso y el examen CCAAK

El curso sigue el temario de la certificación Confluent Certified Administrator for Apache Kafka. El examen dura 90 minutos, se hace en remoto con supervisión y combina preguntas de respuesta única, de respuesta múltiple, de emparejar y de ordenar. La certificación tiene una validez de dos años. Las preguntas no son de memoria pura: suelen describir un clúster y una situación (un broker caído, un consumidor que no avanza, una configuración concreta) y piden razonar qué ocurre o qué hacer.

El temario se organiza en siete dominios: fundamentos de Kafka, configuración del clúster, seguridad, arquitectura de despliegue, Kafka Connect, observabilidad y troubleshooting. Antes de presentarte, descarga la guía oficial del examen desde la web de certificación de Confluent, porque es la referencia vigente.

## 2. Event streaming frente a mensajería tradicional

En una cola de mensajes clásica (JMS, IBM MQ, RabbitMQ en su uso habitual), el mensaje se consume y desaparece. Si un segundo sistema necesita la misma información, hay que duplicarla en otra cola, y el pasado no se puede volver a leer.

Kafka es un **log distribuido**: los eventos se escriben en orden, uno detrás de otro, y se conservan durante el tiempo que marque la retención del topic. Leer no borra. Cada consumidor lleva la cuenta de por dónde va, de modo que muchos sistemas pueden leer los mismos datos a su ritmo, e incluso volver atrás y reprocesar.

| | Mensajería tradicional | Event streaming con Kafka |
|---|---|---|
| Tras leer el mensaje | Desaparece | Sigue disponible hasta que actúa la retención |
| Varios lectores | Requiere duplicar colas | Cada grupo de consumidores lee todo de forma independiente |
| Releer el pasado | No es posible | Es posible dentro de la retención |
| Quién decide cuánto se guarda | El consumo | La configuración del topic |

Un **evento** es la constancia de que algo ha ocurrido: la furgoneta V007 estaba en tal posición a tal hora. Los eventos son hechos inmutables; si la furgoneta se mueve, se escribe un evento nuevo.

Patrones que aparecen en el temario:

- **Publicación/suscripción (pub/sub):** los productores publican en un topic sin saber quién leerá, y los consumidores se suscriben sin saber quién escribe.
- **Event sourcing:** el estado de una entidad se obtiene a partir de la secuencia completa de eventos que la han modificado. El saldo de una cuenta es la suma de sus movimientos.
- **CQRS:** se separa el modelo de escritura (eventos) de las vistas de lectura (índices, tablas agregadas, cachés) que se construyen a partir de ellos.

Casos de uso típicos: integración de microservicios, captura de cambios de bases de datos (CDC), telemetría y logs, analítica en tiempo real, detección de fraude, replicación entre centros de datos y alimentación de data lakes.

## 3. Brokers y clúster: KRaft frente a ZooKeeper

Un clúster de Kafka está formado por **brokers**, procesos que guardan datos en disco y atienden a productores y consumidores. Los clientes se conectan a uno o varios *bootstrap servers*, obtienen los metadatos del clúster y, a partir de ahí, hablan directamente con el broker que necesitan.

Los metadatos (qué brokers hay, qué topics existen, dónde están las réplicas, quién lidera cada partición) los gestiona el **controlador**.

- **Con ZooKeeper** (modelo histórico), los metadatos vivían en un sistema externo y uno de los brokers era elegido controlador a través de él. Había que operar dos sistemas y los cambios de controlador eran lentos en clústeres grandes.
- **Con KRaft**, Kafka gestiona sus propios metadatos. Un conjunto de nodos con rol de *controller* forma un **quórum Raft** y replica el log interno `__cluster_metadata`. Uno de ellos es el **controlador activo** (líder del quórum); los demás están listos para relevarlo en segundos. Los brokers leen ese log como observadores.

Datos clave:

- KRaft está listo para producción desde Kafka 3.3. ZooKeeper se declaró obsoleto en 3.5 y desapareció en **Kafka 4.0 y Confluent Platform 8.0**, que solo funcionan en KRaft. Kafka 3.9 (CP 7.9) es la última versión puente para migrar.
- El quórum funciona **por mayoría**. Con 3 votantes se tolera la caída de 1; con 5, la de 2. Un cuarto votante no aporta tolerancia, por eso se usan números impares.
- Cada nodo se configura con `process.roles`: `broker`, `controller` o ambos (**modo combinado**). El modo combinado es cómodo para desarrollo; en producción se recomiendan controllers dedicados.
- Cada broker envía latidos al controlador. Si deja de hacerlo durante `broker.session.timeout.ms`, el controlador lo aparta (*fenced*) y mueve el liderazgo de sus particiones.
- Si se pierde la mayoría del quórum, los brokers siguen sirviendo con los metadatos que tienen, pero no puede haber cambios de metadatos ni elecciones de líder.

## 4. Topics, particiones, offsets y replicación

Un **topic** es un nombre lógico para un flujo de eventos. Físicamente se divide en **particiones**: cada una es un log ordenado al que solo se añade por el final. Cada evento tiene un **offset**, su posición dentro de la partición; el offset es único dentro de su partición, no entre particiones.

Las particiones permiten escalar (se reparten entre brokers) y marcan el techo de paralelismo de los consumidores.

- **El orden solo está garantizado dentro de una partición.**
- La **clave** del mensaje determina la partición: el particionador calcula un hash de la clave, de modo que misma clave implica misma partición y, por tanto, mismo orden. En el curso la clave es el identificador del vehículo.
- Los mensajes **sin clave** se reparten con el particionado *sticky*: el productor llena un lote para una partición y cambia al siguiente lote, así que con pocos mensajes pueden acabar todos en la misma partición.
- Las particiones se pueden **aumentar, nunca reducir**. Aumentarlas en un topic con clave cambia el reparto (hash módulo un número distinto) y se pierde el orden entre los eventos antiguos y los nuevos de una misma clave.

**Replicación.** Cada partición tiene varias copias; su número es el **factor de replicación** (RF), que incluye al líder. Una réplica es la **líder** y atiende escrituras y lecturas; las demás son **followers** que copian del líder como lo haría un consumidor.

El **ISR** (*in-sync replicas*) es el conjunto de réplicas al día con el líder. Una réplica sale del ISR si no lo alcanza dentro de `replica.lag.time.max.ms` (30 s por defecto). Una partición con menos réplicas en el ISR que su RF está **under-replicated**.

Si cae el líder, el controlador elige uno nuevo **entre las réplicas del ISR**. Si no queda ninguna, decide `unclean.leader.election.enable`:

| Valor | Comportamiento | Prioriza |
|---|---|---|
| `false` (por defecto) | La partición queda offline hasta que vuelva una réplica del ISR | Consistencia |
| `true` | Se elige una réplica desincronizada y se pierden los eventos que no tenía | Disponibilidad |

La **réplica preferida** es la primera de la lista de réplicas de cada partición. Kafka intenta que sea la líder para repartir el liderazgo; lo corrige de forma periódica (`auto.leader.rebalance.enable`) o a demanda con `kafka-leader-election --election-type PREFERRED`.

## 5. Productores y consumidores

**Productor.** Agrupa los mensajes en lotes por partición. El parámetro `acks` decide cuándo un mensaje se considera escrito:

| `acks` | Espera a | Riesgo |
|---|---|---|
| `0` | Nada | No sabe si el mensaje llegó |
| `1` | El líder | Se pierde si el líder cae antes de replicar |
| `all` (`-1`), por defecto desde Kafka 3.0 | Todas las réplicas del ISR | Ninguno, si se combina con un mínimo de ISR adecuado |

`acks=all` por sí solo no basta: si el ISR se ha reducido al líder, se comporta como `acks=1`. Por eso existe `min.insync.replicas`, el número mínimo de réplicas en el ISR para aceptar una escritura con `acks=all`; por debajo, el broker devuelve `NOT_ENOUGH_REPLICAS`.

> **Combinación de referencia: RF 3 · `min.insync.replicas=2` · `acks=all`.** Permite perder un broker sin perder datos ni dejar de aceptar escrituras.

El **productor idempotente** (activo por defecto desde Kafka 3.0) evita duplicados por reintentos y mantiene el orden con varias peticiones en vuelo. Sobre él se construyen las **transacciones**, base del procesamiento *exactly-once*.

**Consumidores.** Leen con llamadas `poll` y confirman su posición (**offset confirmado**), que Kafka guarda en el topic interno `__consumer_offsets`. Se organizan en **grupos** (`group.id`):

- Dentro de un grupo, cada partición la lee un único consumidor. Con más consumidores que particiones, los sobrantes quedan sin asignación.
- Grupos distintos son independientes: cada uno recibe todos los eventos y lleva sus propios offsets.
- Un broker hace de **coordinador** del grupo. Cuando entra o sale un miembro hay un **rebalanceo**. El protocolo clásico (*eager*) detiene a todos los miembros; el cooperativo y el nuevo protocolo de Kafka 4.0 solo mueven lo necesario.

| Parámetro | Detecta |
|---|---|
| `session.timeout.ms` | Consumidores caídos que dejan de enviar latidos |
| `max.poll.interval.ms` | Consumidores vivos pero atascados entre dos `poll` (5 min por defecto) |

## Lo que pregunta el examen

- Leer no borra; la retención la decide el topic.
- El quórum KRaft es por mayoría, con 3 o 5 votantes y un único controlador activo; los metadatos están en `__cluster_metadata`.
- Orden por partición; la clave decide la partición; las particiones solo aumentan.
- El RF incluye al líder; el nuevo líder sale del ISR; `unclean.leader.election.enable` es el compromiso entre disponibilidad y pérdida de datos.
- RF 3, `min.insync.replicas=2` y `acks=all`; idempotencia por defecto.
- Cada partición tiene un solo consumidor por grupo; offsets en `__consumer_offsets`; diferencia entre `session.timeout.ms` y `max.poll.interval.ms`.

## Para practicar

- Simulacro del Bloque 1 (`03_Simulacros_por_bloque.docx`).
- Lab 1 (Bloque 2): [`labs/lab01-cluster-kraft`](../labs/lab01-cluster-kraft/README.md).

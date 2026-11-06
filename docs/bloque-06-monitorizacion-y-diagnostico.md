# Bloque 6 · Monitorización y diagnóstico

**Sesiones 6 y 7 · miércoles 11 y jueves 12 de noviembre** · Presentación: [`presentaciones/Bloque_06_Monitorizacion_y_diagnostico.pptx`](../presentaciones/Bloque_06_Monitorizacion_y_diagnostico.pptx) · Laboratorio: [Lab 7](../labs/lab07-prometheus-grafana/README.md)

Este bloque enseña a saber si un clúster está sano y, cuando no lo está, a encontrar la causa: lag de consumidores y alertas, herramientas de monitorización, diagnóstico con la línea de comandos, incidencias habituales y recuperación de brokers y del clúster.

## 1. Consumer lag, caudal y alertas

El **lag** de un grupo en una partición es la diferencia entre el último offset escrito (*log end offset*) y el último offset confirmado por el grupo: los mensajes pendientes de leer.

Lo importante es su tendencia:

- **Diente de sierra:** normal (lectura y confirmación por lotes).
- **Alto pero estable:** aceptable si el negocio tolera ese retraso.
- **Creciente de forma sostenida:** el grupo lee más despacio de lo que se escribe y acabará perdiendo datos cuando actúe la retención.

Medirlo también en tiempo (segundos de retraso) suele ser más útil que en mensajes.

Fuentes del lag:

- **Desde fuera:** offsets confirmados en `__consumer_offsets` comparados con el final de cada partición (`kafka-consumer-groups`, Control Center, Confluent Cloud, `kafka-exporter`). Funciona aunque el consumidor esté caído.
- **Desde dentro:** métricas del cliente como `records-lag-max`, que desaparecen con el cliente.

**Alertas.** Separar dos niveles y exigir siempre una duración mínima antes de disparar:

| Nivel | Condiciones |
|---|---|
| Crítica | Particiones offline; particiones bajo `min.insync.replicas`; `ActiveControllerCount` distinto de 1; broker caído; directorio de datos offline; lag sostenido de consumidores críticos |
| Aviso | Particiones under-replicated durante minutos; contracciones frecuentes del ISR; hilos por encima del 70 % de ocupación; disco por encima del 70 %; latencias anómalas; tareas de Connect fallidas |

## 2. Herramientas de monitorización

| Herramienta | Entorno | Qué ofrece |
|---|---|---|
| Confluent Control Center | Confluent Platform | Consola web de gestión y monitorización: salud, rendimiento, lag, conectores, ksqlDB, esquemas y alertas |
| Health+ | Confluent Platform | Servicio en la nube que recibe telemetría (Telemetry Reporter, sin datos de mensajes) y ofrece paneles, alertas y recomendaciones |
| JMX Exporter + Prometheus + Grafana | Cualquiera | Estándar abierto: el agente expone las métricas JMX en formato Prometheus; Prometheus las guarda y evalúa alertas; Grafana las visualiza |
| Metrics API | Confluent Cloud | Métricas de los clústeres gestionados, integrables con Prometheus, Datadog y otros |

## 3. Diagnóstico con la línea de comandos

| Herramienta | Opciones clave |
|---|---|
| `kafka-topics --describe` | `--under-replicated-partitions`, `--under-min-isr-partitions`, `--at-min-isr-partitions`, `--unavailable-partitions`, `--topics-with-overrides` |
| `kafka-consumer-groups` | `--list`; `--describe --group` (offset, final del log, lag, consumidor); `--members`; `--state` (`Stable`, `PreparingRebalance`, `CompletingRebalance`, `Empty`, `Dead`); `--reset-offsets` (con `--dry-run` y `--execute`, **solo con el grupo inactivo**); `--delete-offsets`, `--delete` |
| `kafka-log-dirs --describe` | Tamaño de cada partición por broker y directorio; directorios con errores |
| `kafka-metadata-quorum` | `describe --status` (líder, época, votantes) y `--replication` (retraso de cada nodo) |
| Otras | `kafka-broker-api-versions` (brokers que responden), `kafka-get-offsets`, `kafka-dump-log`, `kafka-configs --describe`, `kafka-features` |

**Método en cuatro preguntas:**

1. ¿Están vivos todos los brokers y controllers, y hay un controlador activo?
2. ¿Hay particiones offline, bajo el mínimo del ISR o under-replicated, y en qué brokers?
3. ¿Qué les pasa a los consumidores afectados: lag, miembros, estado del grupo?
4. ¿Qué dicen los logs del broker o del cliente?

## 4. Incidencias comunes

| Incidencia | Cómo se manifiesta | Causas típicas |
|---|---|---|
| Broker caído | Menos brokers activos, URP > 0 | Proceso caído, hardware, memoria. Con RF 3 y min ISR 2 no hay corte |
| Partición offline | `OfflinePartitionsCount` > 0, `--unavailable-partitions` | Todas las réplicas del ISR caídas; topics con RF 1 |
| Partición bajo el mínimo del ISR | `NOT_ENOUGH_REPLICAS` en productores `acks=all`; se puede leer | Réplicas caídas o retrasadas; RF igual a `min.insync.replicas` |
| Consumidor que no avanza | Lag creciente con el clúster sano | Sin miembros activos; mensaje que no sabe procesar; procesamiento que supera `max.poll.interval.ms`; falta de permisos sobre el grupo |
| Disco lleno | Directorio offline; si es el único, el broker se detiene | Topics sin límite de retención; retención mal calculada; compactación parada |
| Rebalanceos continuos | El grupo alterna estados de rebalanceo y la generación crece | Reinicios en bucle, pausas largas de GC, `max.poll.interval.ms` o `session.timeout.ms` ajustados |
| Otras | Errores de conexión, *throttling*, `RecordTooLargeException`, hilos saturados | `advertised.listeners`, certificados caducados, cuotas, `max.message.bytes`, falta de capacidad |

## 5. Recuperar fallos de broker y de clúster

**Broker caído.** Antes de arrancarlo, averiguar la causa (logs del broker y del sistema, disco, memoria).

- Si vuelve con sus datos, se pone al día copiando de los líderes y entra de nuevo en el ISR; después, elección de líder preferido.
- Si se ha perdido el disco, se arranca con el mismo `node.id`, el disco vacío formateado con el mismo identificador de clúster, y recrea sus réplicas por replicación (conviene limitarla con throttle).
- Si no va a volver, se reasignan sus réplicas antes de darlo de baja.

**Controllers (KRaft).** Con uno de tres caído, el quórum sigue funcionando. Con dos de tres caídos, no hay controlador activo: no hay cambios de metadatos ni elecciones de líder, y cualquier fallo adicional de broker deja particiones sin líder. La prioridad es recuperar la mayoría. En versiones recientes, el quórum dinámico permite añadir y retirar controllers con `kafka-metadata-quorum`.

**Arranque completo** (por ejemplo, tras un corte eléctrico): primero los controllers, hasta tener quórum y controlador activo; después los brokers.

**Validación de que el clúster está sano:** todos los brokers registrados, exactamente un controlador activo, ninguna partición offline ni bajo el mínimo del ISR, URP volviendo a cero, consumidores críticos reduciendo su lag y una prueba real de escritura y lectura con `acks=all`.

## Lo que pregunta el examen

- Definición de lag; el lag creciente como síntoma; desde dónde se mide.
- Qué métricas merecen alerta crítica.
- Para qué sirve cada herramienta de monitorización y en qué entorno.
- Opciones de `kafka-topics` y `kafka-consumer-groups`; el reset de offsets exige grupo inactivo.
- Broker que vuelve con datos frente a broker que vuelve vacío; pérdida del quórum; orden de arranque.

## Para practicar

- Simulacro del Bloque 6.
- Lab 7: [`labs/lab07-prometheus-grafana`](../labs/lab07-prometheus-grafana/README.md).

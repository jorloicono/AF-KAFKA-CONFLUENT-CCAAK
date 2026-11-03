# Lab 6 · Clúster Connect distribuido con un pipeline PostgreSQL → Kafka; provocar fallos y recuperarlos

**Bloque 5 · Duración estimada: 75–90 minutos**

## Objetivo

Kafka Connect es la pieza que más incidencias genera en el día a día de un administrador, porque depende de sistemas externos que fallan, de configuraciones que cambian y de datos que no siempre llegan como se espera. En este laboratorio montamos un clúster Connect de dos workers en modo distribuido, desplegamos un conector de origen que lleva una tabla de PostgreSQL a Kafka y un conector de destino que la copia de vuelta, y después lo rompemos de cuatro formas distintas para practicar el diagnóstico con la API REST, los logs y la dead letter queue.

## Punto de partida

```bash
cd labs/lab06-connect
docker compose up -d
docker logs -f connect-1 2>&1 | grep -iE "Kafka Connect started|ERROR"     # Ctrl+C cuando arranque
```

La primera vez los workers tardan un par de minutos porque descargan el conector JDBC desde Confluent Hub. Cuando estén listos, comprueba desde tu equipo que los dos workers responden y que el plugin está instalado (en PowerShell, `curl.exe`):

```bash
curl -s localhost:8083/ ; echo
curl -s localhost:8084/ ; echo
curl -s localhost:8083/connector-plugins | tr ',' '\n' | grep class
```

## Paso 1 · Los topics internos

Un clúster Connect distribuido guarda todo su estado en tres topics de Kafka: las configuraciones de los conectores, los offsets de los conectores de origen y el estado de conectores y tareas. Por eso cualquier worker puede asumir el trabajo de otro.

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topics-with-overrides | grep _connect
```

Observa que los tres son compactados (`cleanup.policy=compact`) y que el de offsets tiene muchas más particiones que los demás (25 por defecto). En producción los tres deben tener factor de replicación 3; aquí usamos 1 porque el entorno tiene un solo broker.

## Paso 2 · Desplegar el conector de origen

```bash
curl -s -X POST -H "Content-Type: application/json" --data @conectores/pg-source-mantenimientos.json localhost:8083/connectors ; echo
curl -s localhost:8083/connectors ; echo
curl -s localhost:8083/connectors/pg-source-mantenimientos/status ; echo
```

El estado muestra el conector y su tarea en `RUNNING`, y el `worker_id` de cada uno. Fíjate en que puedes consultar el mismo conector desde el puerto 8084: los dos workers forman un único clúster porque comparten `group.id`. Ahora consume el topic:

```bash
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 --topic pg.mantenimientos \
  --from-beginning --max-messages 3 --property print.key=true
```

Cada mensaje lleva el esquema y los datos en JSON (porque el worker usa `JsonConverter` con `schemas.enable=true`) y la clave es el `id` de la fila, gracias a las dos transformaciones SMT del conector (`ValueToKey` y `ExtractField$Key`). Inserta una fila nueva y comprueba que llega en un par de segundos:

```bash
docker exec postgres psql -U flota -d flota -c "INSERT INTO mantenimientos (vehiculo_id, tipo, coste_euros) VALUES ('V011','Frenos',290);"
```

## Paso 3 · Desplegar el conector de destino con DLQ

```bash
curl -s -X POST -H "Content-Type: application/json" --data @conectores/pg-sink-copia.json localhost:8083/connectors ; echo
curl -s localhost:8083/connectors/pg-sink-copia/status ; echo
docker exec postgres psql -U flota -d flota -c "SELECT * FROM mantenimientos_copia ORDER BY id;"
```

El conector de destino es en realidad un consumidor: su grupo se llama `connect-pg-sink-copia` y puedes ver su lag como el de cualquier otro grupo:

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group connect-pg-sink-copia
```

## Fallo 1 · La base de datos de origen desaparece

```bash
docker stop postgres
sleep 40
curl -s localhost:8083/connectors/pg-source-mantenimientos/status ; echo
```

Dependiendo de la versión del conector y de su política de reintentos (`query.retry.attempts`, `connection.attempts`), verás una de dos cosas, y las dos son instructivas. Si la tarea pasa a `FAILED`, el campo `trace` contiene la excepción. Si sigue en `RUNNING`, el log del worker se llena de errores de conexión mientras la API dice que todo va bien, lo que demuestra que `RUNNING` no significa "moviendo datos" y que hay que vigilar también métricas y logs. En ambos casos, mira el log del worker que ejecutaba la tarea (`docker logs connect-1` o `connect-2`, según el `worker_id`). Recupera la base de datos y reinicia solo lo que ha fallado:

```bash
docker start postgres
sleep 10
curl -s -X POST "localhost:8083/connectors/pg-source-mantenimientos/restart?includeTasks=true&onlyFailed=true" ; echo
curl -s localhost:8083/connectors/pg-source-mantenimientos/status ; echo
```

Una tarea `FAILED` no se recupera sola cuando el sistema externo vuelve: alguien (o algo, como un script de monitorización) tiene que reiniciarla.

## Fallo 2 · Cae un worker

Mira en qué worker corre cada tarea y detén ese worker:

```bash
curl -s localhost:8084/connectors/pg-source-mantenimientos/status ; echo
docker stop connect-1
sleep 30
curl -s localhost:8084/connectors/pg-source-mantenimientos/status ; echo
curl -s localhost:8084/connectors/pg-sink-copia/status ; echo
```

Las tareas que estaban en `connect-1` se reasignan a `connect-2`. Con el rebalanceo cooperativo de Connect, el reparto espera `scheduled.rebalance.max.delay.ms` por si el worker vuelve (5 minutos por defecto; en este laboratorio lo hemos bajado a 20 segundos). Vuelve a arrancar el worker:

```bash
docker start connect-1
```

## Fallo 3 · Una configuración errónea

Crea un conector con una contraseña incorrecta usando `PUT /connectors/{nombre}/config`, que crea o actualiza de forma idempotente:

```bash
curl -s -X PUT -H "Content-Type: application/json" --data @conectores/pg-source-config-erronea.json localhost:8083/connectors/pg-source-v2/config ; echo
sleep 15
curl -s localhost:8083/connectors/pg-source-v2/status ; echo
```

Lee el `trace` hasta encontrar el error de autenticación de PostgreSQL. Corrige la configuración con el mismo verbo y reinicia:

```bash
curl -s -X PUT -H "Content-Type: application/json" --data @conectores/pg-source-config-buena.json localhost:8083/connectors/pg-source-v2/config ; echo
curl -s -X POST "localhost:8083/connectors/pg-source-v2/restart?includeTasks=true" ; echo
curl -s localhost:8083/connectors/pg-source-v2/status ; echo
```

## Fallo 4 · Un mensaje envenenado y la dead letter queue

Escribimos en el topic de origen del conector de destino un mensaje que no es JSON:

```bash
docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:29092 --topic pg.mantenimientos <<< 'esto no es json'
sleep 5
curl -s localhost:8083/connectors/pg-sink-copia/status ; echo
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 --topic dlq.mantenimientos \
  --from-beginning --max-messages 1 --property print.headers=true
```

El conector sigue en `RUNNING` porque tiene `errors.tolerance=all`, y el mensaje defectuoso está en `dlq.mantenimientos` con cabeceras `__connect.errors.*` que explican la etapa (`VALUE_CONVERTER`), la clase de excepción y el mensaje. Sin esa configuración, la tarea habría pasado a `FAILED` y se habría quedado parada en ese offset. Recuerda que la DLQ solo existe para conectores de destino y que solo captura errores de conversión y de transformación, no los fallos del sistema externo al escribir.

## Paso 4 · Cambiar el nivel de log en caliente

```bash
curl -s -X PUT -H "Content-Type: application/json" --data @conectores/log-debug.json localhost:8083/admin/loggers/io.confluent.connect.jdbc ; echo
docker logs --tail 20 connect-1
curl -s -X PUT -H "Content-Type: application/json" --data @conectores/log-info.json localhost:8083/admin/loggers/io.confluent.connect.jdbc ; echo
```

## Paso 5 · Pausar, reanudar y borrar

```bash
curl -s -X PUT localhost:8083/connectors/pg-source-v2/pause
curl -s localhost:8083/connectors/pg-source-v2/status ; echo
curl -s -X PUT localhost:8083/connectors/pg-source-v2/resume
curl -s -X DELETE localhost:8083/connectors/pg-source-v2
curl -s localhost:8083/connectors ; echo
```

## Preguntas de comprobación

1. ¿Qué tres topics internos usa un clúster Connect distribuido y qué guarda cada uno?
2. ¿Qué diferencia hay entre un connector y una task, y cuál de los dos mueve datos?
3. ¿Qué tiene que pasar para que una tarea `FAILED` vuelva a `RUNNING`?
4. ¿Qué tipo de errores captura la DLQ y en qué tipo de conectores existe?
5. ¿Por qué tardan en reasignarse las tareas de un worker caído y qué parámetro lo controla?

## Limpieza

```bash
docker compose down -v
```

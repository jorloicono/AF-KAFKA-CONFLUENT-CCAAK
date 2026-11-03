# Lab 5 · Cambiar el modo de compatibilidad y provocar un cambio de esquema rechazado

**Bloque 4 · Duración estimada: 45–60 minutos**

## Objetivo

Schema Registry es el guardián del contrato entre productores y consumidores. En este laboratorio vamos a registrar varias versiones de un esquema Avro de posiciones de la flota, a ver cómo el modo de compatibilidad decide qué cambios se aceptan y cuáles se rechazan, y a practicar las tareas de administración que pregunta el examen: cambiar la compatibilidad global y por subject, borrar subjects de forma lógica y física, poner el registro en solo lectura y entender el papel del topic `_schemas` y de la elección de líder entre nodos.

## Punto de partida

```bash
cd labs/lab05-schema-registry
docker compose up -d
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create \
  --topic flota.posiciones --partitions 3 --replication-factor 1
curl -s http://localhost:8081/subjects ; echo
```

Creamos el topic de forma explícita antes de empezar, que es la práctica recomendada en producción aunque el broker de este entorno ligero permita la creación automática. Si el `curl` devuelve un error de conexión, espera unos segundos: Schema Registry no responde hasta que ha leído el topic `_schemas`.

Si estás en PowerShell, usa `curl.exe` en lugar de `curl` en todos los comandos, porque `curl` a secas es un alias de `Invoke-WebRequest`. En la carpeta `schemas/` tienes cada versión del esquema en dos formatos: el `.avsc` legible y el `peticion-*.json` listo para enviar a la API.

Para no repetir cabeceras, define estas variables en tu terminal (en PowerShell, escribe la URL completa en cada comando):

```bash
SR=http://localhost:8081
CT="Content-Type: application/vnd.schemaregistry.v1+json"
SUBJ=flota.posiciones-value
```

## Paso 1 · Compatibilidad global y primera versión

```bash
curl -s $SR/config ; echo
curl -s -X POST -H "$CT" --data @schemas/peticion-v1.json $SR/subjects/$SUBJ/versions ; echo
curl -s $SR/subjects ; echo
curl -s $SR/subjects/$SUBJ/versions/latest ; echo
```

La compatibilidad global es `BACKWARD`, que es el valor por defecto de Schema Registry. El nombre del subject sigue la estrategia por defecto, `TopicNameStrategy`: nombre del topic más el sufijo `-value` (o `-key` para la clave).

## Paso 2 · Un cambio compatible

La versión 2 añade el campo `combustible` como opcional y con valor por defecto. Antes de registrarla, preguntamos si es compatible:

```bash
curl -s -X POST -H "$CT" --data @schemas/peticion-v2.json "$SR/compatibility/subjects/$SUBJ/versions/latest?verbose=true" ; echo
curl -s -X POST -H "$CT" --data @schemas/peticion-v2.json $SR/subjects/$SUBJ/versions ; echo
```

## Paso 3 · Un cambio rechazado

La versión 3 añade el campo `conductor` como obligatorio y sin valor por defecto. Con `BACKWARD`, el esquema nuevo tiene que poder leer los datos escritos con el anterior, y esos datos no traen `conductor`, así que no hay forma de rellenarlo:

```bash
curl -s -X POST -H "$CT" --data @schemas/peticion-v3.json "$SR/compatibility/subjects/$SUBJ/versions/latest?verbose=true" ; echo
curl -s -w "\nHTTP %{http_code}\n" -X POST -H "$CT" --data @schemas/peticion-v3.json $SR/subjects/$SUBJ/versions
```

El registro devuelve `HTTP 409` con el código de error 409 y la explicación del motivo. Así es como Schema Registry impide que un productor rompa a los consumidores existentes.

## Paso 4 · Cambiar el modo de compatibilidad del subject

Supongamos que en este topic lo prioritario es que los consumidores antiguos sigan funcionando cuando los productores se actualicen primero. Eso es compatibilidad `FORWARD`. La cambiamos solo para este subject, sin tocar la global:

```bash
curl -s -X PUT -H "$CT" --data @schemas/compat-forward.json $SR/config/$SUBJ ; echo
curl -s $SR/config/$SUBJ ; echo
curl -s $SR/config ; echo
curl -s -X POST -H "$CT" --data @schemas/peticion-v3.json $SR/subjects/$SUBJ/versions ; echo
```

Ahora la versión 3 se acepta, porque un lector con la versión 2 simplemente ignora el campo `conductor`. Prueba la versión 4, que elimina `velocidad`, un campo sin valor por defecto:

```bash
curl -s -w "\nHTTP %{http_code}\n" -X POST -H "$CT" --data @schemas/peticion-v4.json $SR/subjects/$SUBJ/versions
```

Con `FORWARD` se rechaza, porque los consumidores con la versión anterior esperan encontrar `velocidad`. Resume para ti la regla práctica: con `BACKWARD` actualizas primero los consumidores; con `FORWARD`, primero los productores; con `FULL` da igual el orden, pero solo puedes añadir o quitar campos que tengan valor por defecto. Las variantes `_TRANSITIVE` comprueban contra todas las versiones anteriores y no solo contra la última.

## Paso 5 · Producir y consumir con Avro

```bash
docker exec -it schema-registry bash

kafka-avro-console-producer --bootstrap-server kafka1:29092 --topic flota.posiciones \
  --property schema.registry.url=http://localhost:8081 \
  --property value.schema.id=3
```

Escribe una línea válida y después una que no cumple el esquema (por ejemplo, sin `conductor`):

```json
{"vehiculo_id":"V001","ts":1730000000000,"velocidad":88,"combustible":{"int":70},"conductor":"Ana"}
{"vehiculo_id":"V002","ts":1730000000000,"velocidad":95}
```

La segunda provoca un error de serialización en el propio productor, antes de llegar al broker. Sal con `Ctrl+D` o `Ctrl+C` y consume:

```bash
kafka-avro-console-consumer --bootstrap-server kafka1:29092 --topic flota.posiciones \
  --property schema.registry.url=http://localhost:8081 --from-beginning --max-messages 1
```

Si el identificador 3 no corresponde a la versión 3 en tu registro, consulta el `id` con `curl -s $SR/subjects/$SUBJ/versions/3`.

## Paso 6 · El topic `_schemas`

Todo el estado de Schema Registry vive en un topic compactado de Kafka. Los nodos del registro no tienen base de datos propia:

```bash
kafka-console-consumer --bootstrap-server kafka1:29092 --topic _schemas --from-beginning \
  --property print.key=true --timeout-ms 5000
exit
```

## Paso 7 · Borrado lógico y físico de subjects

```bash
curl -s -X POST -H "$CT" --data @schemas/peticion-v1.json $SR/subjects/pruebas-value/versions ; echo
curl -s -X DELETE $SR/subjects/pruebas-value ; echo
curl -s $SR/subjects ; echo
curl -s "$SR/subjects?deleted=true" ; echo
curl -s -X DELETE "$SR/subjects/pruebas-value?permanent=true" ; echo
curl -s "$SR/subjects?deleted=true" ; echo
```

El primer `DELETE` es un borrado lógico (*soft delete*): el subject desaparece del listado normal pero sigue existiendo y los identificadores de esquema siguen siendo resolubles. El borrado físico (*hard delete*, `permanent=true`) solo se permite después del lógico y es irreversible.

## Paso 8 · Modo de solo lectura y segundo nodo

```bash
curl -s -X PUT -H "$CT" --data @schemas/modo-readonly.json $SR/mode ; echo
curl -s -w "\nHTTP %{http_code}\n" -X POST -H "$CT" --data @schemas/peticion-v2.json $SR/subjects/otro-value/versions
curl -s -X PUT -H "$CT" --data @schemas/modo-readwrite.json $SR/mode ; echo
```

El modo `READONLY` se usa, por ejemplo, en el registro secundario de una arquitectura con dos centros, o durante una migración (junto con el modo `IMPORT`). Para ver la elección de líder, arranca el segundo nodo y registra un esquema a través de él:

```bash
docker compose --profile ha up -d schema-registry-2
curl -s -X POST -H "$CT" --data @schemas/peticion-v1.json http://localhost:8082/subjects/otro-value/versions ; echo
docker logs schema-registry-2 2>&1 | grep -i leader | tail -3
```

Los dos nodos comparten `schema.registry.group.id`, uno de ellos es el líder y es el único que escribe en `_schemas`; el otro reenvía las escrituras al líder. Un nodo con `leader.eligibility=false` nunca se presenta a líder, que es la configuración típica del registro de un centro secundario.

## Preguntas de comprobación

1. ¿Qué tipo de cambio rechaza `BACKWARD` y por qué?
2. Con `FORWARD`, ¿qué se actualiza primero, productores o consumidores?
3. ¿Qué diferencia hay entre `FULL` y `FULL_TRANSITIVE`?
4. ¿Qué pasos son necesarios para borrar físicamente un subject?
5. ¿Dónde guarda Schema Registry los esquemas y qué ocurre si se pierde ese topic?

## Limpieza

```bash
docker compose --profile ha down -v
```

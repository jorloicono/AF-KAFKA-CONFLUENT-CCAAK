# Lab 3 · Proteger el clúster con TLS y SASL/SCRAM y dar permisos con ACLs

**Bloque 4 · Duración estimada: 75–90 minutos**

## Objetivo

Vamos a pasar de un clúster abierto a uno con las tres capas de seguridad que el examen trata por separado: cifrado en tránsito con TLS, autenticación (con certificados para los brokers y el administrador, y con SASL/SCRAM para las aplicaciones) y autorización con ACLs. La idea es que al terminar seas capaz de leer un mensaje de error y saber a qué capa pertenece: un fallo de *handshake* TLS, un `SaslAuthenticationException` o un `TopicAuthorizationException` cuentan historias muy distintas.

## Arquitectura del laboratorio

El clúster tiene tres nodos KRaft y tres listeners. `BROKER` (puerto 29092) usa SSL con autenticación mutua, y lo usan los brokers para replicar entre sí y el administrador, que se identifica con un certificado cuyo CN es `admin`. `CLIENT` (puerto 19092) usa SASL_SSL con SCRAM-SHA-512 y es el que usan las aplicaciones. `CONTROLLER` (puerto 29093) es el del quórum KRaft y lo dejamos en PLAINTEXT dentro de la red de Docker para simplificar; en producción también iría cifrado. La regla `ssl.principal.mapping.rules` convierte el DN `CN=admin,O=Formacion,C=ES` en el principal `User:admin`, y los superusuarios son los tres brokers, el administrador y `ANONYMOUS` (este último solo es alcanzable a través del listener del controller, que no se publica).

## Paso 1 · Generar los certificados

Desde la carpeta del laboratorio, genera la CA y los almacenes con `keytool` dentro de un contenedor:

```bash
cd labs/lab03-tls-scram-acl
docker run --rm --user root -v "$PWD":/lab -w /lab --entrypoint bash \
  confluentinc/cp-server:7.9.0 generar-certs.sh
```

En PowerShell usa `-v "${PWD}:/lab"`. Al terminar tendrás en `secrets/` un truststore con la CA y un keystore por cada identidad. Inspecciona uno para ver el sujeto, el emisor y los nombres alternativos (SAN), que son los que el cliente compara con el nombre del host al validar el certificado:

```bash
docker run --rm -v "$PWD/secrets":/s --entrypoint keytool confluentinc/cp-server:7.9.0 \
  -list -v -keystore /s/kafka1.keystore.jks -storepass confluent | grep -E "Alias|Owner|Issuer|DNSName"
```

## Paso 2 · Arrancar el clúster

```bash
docker compose up -d
docker compose ps
docker logs kafka1 2>&1 | grep -iE "started|error" | tail
docker exec -it cliente bash
```

## Paso 3 · El administrador entra con certificado

Desde el contenedor `cliente`, prueba primero a conectar sin TLS y después con TLS pero sin certificado de cliente. Los dos intentos deben fallar, cada uno por un motivo distinto:

```bash
kafka-topics --bootstrap-server kafka1:29092 --list
kafka-topics --bootstrap-server kafka1:29092 --command-config /clientes/sin-certificado.properties --list
```

El primero se queda esperando o falla porque intenta hablar en texto plano con un puerto que espera TLS; el segundo completa el cifrado, pero el broker lo rechaza porque `ssl.client.auth=required`. Ahora con el certificado del administrador:

```bash
kafka-topics --bootstrap-server kafka1:29092 --command-config /clientes/admin-ssl.properties --list
kafka-topics --bootstrap-server kafka1:29092 --command-config /clientes/admin-ssl.properties \
  --create --topic flota.posiciones --partitions 3 --replication-factor 3
```

## Paso 4 · Crear usuarios SCRAM

Las credenciales SCRAM no se guardan en ficheros sino en los metadatos del clúster, con sal y hash. Las crea el administrador:

```bash
kafka-configs --bootstrap-server kafka1:29092 --command-config /clientes/admin-ssl.properties \
  --alter --entity-type users --entity-name alice \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=alice-secret]'
kafka-configs --bootstrap-server kafka1:29092 --command-config /clientes/admin-ssl.properties \
  --alter --entity-type users --entity-name bob \
  --add-config 'SCRAM-SHA-512=[iterations=8192,password=bob-secret]'
kafka-configs --bootstrap-server kafka1:29092 --command-config /clientes/admin-ssl.properties \
  --describe --entity-type users
```

## Paso 5 · Autenticada, pero no autorizada

Alice ya puede autenticarse, pero todavía no tiene ningún permiso. Intenta producir con ella por el listener `CLIENT`:

```bash
bash /scripts/generar-flota.sh 10 kafka1:19092 flota.posiciones /clientes/alice.properties
```

El error es `TopicAuthorizationException`: la autenticación ha funcionado y lo que falla es la autorización, porque con `allow.everyone.if.no.acl.found=false` lo que no está permitido explícitamente está prohibido. Compáralo con el error de una contraseña incorrecta:

```bash
kafka-topics --bootstrap-server kafka1:19092 --command-config /clientes/mallory.properties --list
```

Aquí el mensaje es de autenticación (`SaslAuthenticationException` / `Authentication failed`).

## Paso 6 · Conceder permisos con ACLs

Las opciones `--producer` y `--consumer` de `kafka-acls` son atajos que conceden de una vez las operaciones que necesita cada tipo de cliente:

```bash
ADMIN="--bootstrap-server kafka1:29092 --command-config /clientes/admin-ssl.properties"

kafka-acls $ADMIN --add --allow-principal User:alice --producer --topic flota.posiciones
kafka-acls $ADMIN --add --allow-principal User:bob --consumer --topic flota.posiciones --group flota-app
kafka-acls $ADMIN --list --topic flota.posiciones
kafka-acls $ADMIN --list --principal User:bob
```

Fíjate en lo que ha creado cada atajo: para Alice, `WRITE`, `DESCRIBE` y `CREATE` sobre el topic; para Bob, `READ` y `DESCRIBE` sobre el topic y `READ` sobre el grupo. Ahora repite las pruebas:

```bash
bash /scripts/generar-flota.sh 200 kafka1:19092 flota.posiciones /clientes/alice.properties

kafka-console-consumer --bootstrap-server kafka1:19092 --consumer.config /clientes/bob.properties \
  --topic flota.posiciones --group flota-app --from-beginning --max-messages 5

kafka-console-consumer --bootstrap-server kafka1:19092 --consumer.config /clientes/bob.properties \
  --topic flota.posiciones --group otro-grupo --from-beginning --max-messages 5

bash /scripts/generar-flota.sh 10 kafka1:19092 flota.posiciones /clientes/bob.properties
```

La primera lectura funciona; la segunda falla con `GroupAuthorizationException` porque Bob no tiene permiso sobre `otro-grupo`; la escritura de Bob falla con `TopicAuthorizationException`. Este patrón de "el topic sí, el grupo no" es una de las preguntas de troubleshooting más típicas del examen.

## Paso 7 · ACLs con prefijo y la regla de DENY

En lugar de dar permisos topic a topic, se pueden conceder sobre un prefijo:

```bash
kafka-topics $ADMIN --create --topic flota.alertas --partitions 3 --replication-factor 3
kafka-acls $ADMIN --add --allow-principal User:alice --operation Write --operation Describe \
  --topic flota. --resource-pattern-type prefixed
kafka-acls $ADMIN --add --allow-principal User:bob --operation Read --operation Describe \
  --topic flota. --resource-pattern-type prefixed
bash /scripts/generar-flota.sh 50 kafka1:19092 flota.alertas /clientes/alice.properties
```

Ahora añade una denegación explícita de lectura de `flota.alertas` para Bob y comprueba que, aunque tiene un `ALLOW` por prefijo, la denegación gana:

```bash
kafka-acls $ADMIN --add --deny-principal User:bob --operation Read --topic flota.alertas
kafka-console-consumer --bootstrap-server kafka1:19092 --consumer.config /clientes/bob.properties \
  --topic flota.alertas --group flota-app --from-beginning --max-messages 5
```

La regla que tienes que memorizar es que un `DENY` aplicable siempre prevalece sobre cualquier `ALLOW`.

## Paso 8 · Limpieza de ACLs

```bash
kafka-acls $ADMIN --remove --deny-principal User:bob --operation Read --topic flota.alertas --force
kafka-acls $ADMIN --list
```

## Para pensar (cifrado en reposo y auditoría)

Este laboratorio cifra el tráfico, pero los segmentos en disco siguen en claro. El cifrado en reposo se resuelve fuera de Kafka (cifrado de volumen o de disco en el sistema operativo o en la nube) o a nivel de campo en el cliente. La trazabilidad de quién hizo qué la aportan los logs de auditoría de Confluent Platform, que veremos en el siguiente laboratorio junto con RBAC.

## Preguntas de comprobación

1. ¿Qué diferencia hay entre `SaslAuthenticationException` y `TopicAuthorizationException`, y qué revisarías en cada caso?
2. ¿Qué operaciones concede `--consumer` y por qué hace falta permiso sobre el grupo?
3. ¿Dónde se almacenan las credenciales SCRAM en un clúster KRaft?
4. ¿Qué ocurre si hay un `ALLOW` por prefijo y un `DENY` literal sobre el mismo topic?
5. ¿Para qué sirve `ssl.principal.mapping.rules`?

## Limpieza

```bash
docker compose down -v
```

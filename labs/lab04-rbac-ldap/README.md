# Lab 4 · RBAC con LDAP: roles sobre topics y grupos

**Bloque 4 · Duración estimada: 60 minutos**

## Objetivo

En el laboratorio anterior gestionamos permisos con ACLs, que funcionan bien pero escalan mal: cada permiso es una regla individual sobre un principal concreto. Confluent Platform añade RBAC (control de acceso basado en roles), en el que asignamos roles predefinidos a usuarios o, mejor aún, a grupos del directorio corporativo. En este laboratorio montamos un LDAP con usuarios y grupos, un Confluent Server con el Metadata Service (MDS) conectado a ese LDAP, y comprobamos cómo los permisos se heredan por pertenencia a un grupo, cómo conviven RBAC y ACLs, y dónde queda la traza en los logs de auditoría.

## Lo que hay en el directorio

Los usuarios son `superuser`, `alice`, `bob`, `carol` y `dave`, con contraseña `<usuario>-secret`. Hay dos grupos: `flota-devs`, al que pertenecen Alice y Carol, y `analistas`, al que pertenece Bob. Dave no pertenece a ningún grupo.

## Paso 1 · Claves del MDS y arranque

El MDS firma los tokens de sesión con un par de claves RSA que generamos antes de arrancar:

```bash
cd labs/lab04-rbac-ldap
docker run --rm -v "$PWD":/lab -w /lab alpine:3.20 sh generar-claves-mds.sh
docker compose up -d
docker logs -f kafka1 2>&1 | grep -iE "started|metadata server|error"     # Ctrl+C cuando arranque
docker exec -it cliente bash
```

## Paso 2 · Explorar el MDS

El MDS expone una API REST en el puerto 8090. Pide el identificador del clúster (lo necesitamos para todos los ámbitos de RBAC) y la lista de roles predefinidos:

```bash
bash /lab/rbac.sh id
bash /lab/rbac.sh roles
```

Localiza en la lista los roles que más aparecen en el examen. A nivel de clúster están `SystemAdmin` (todo), `ClusterAdmin` (gestión del clúster sin acceso a datos), `UserAdmin` (gestión de role bindings), `SecurityAdmin`, `AuditAdmin` y `Operator`. A nivel de recurso están `ResourceOwner` (control total de un recurso, incluido conceder acceso a otros), `DeveloperManage` (crear y configurar), `DeveloperRead`, `DeveloperWrite`.

## Paso 3 · Sin roles no hay acceso

Comprueba que Alice se autentica contra LDAP pero no puede hacer nada todavía, y que un usuario con contraseña incorrecta ni siquiera se autentica:

```bash
kafka-topics --bootstrap-server kafka1:19092 --command-config /clientes/alice.properties --list
kafka-topics --bootstrap-server kafka1:19092 --command-config /clientes/alice.properties \
  --create --topic flota.posiciones --partitions 3 --replication-factor 1
```

La lista sale vacía (no puede ver ningún topic) y la creación falla por autorización.

## Paso 4 · Role bindings para grupos y usuarios

Damos al grupo `flota-devs` capacidad para crear y escribir en cualquier topic que empiece por `flota.`, y a Bob capacidad para leer esos topics usando grupos de consumidores que empiecen por `analitica`:

```bash
bash /lab/rbac.sh asignar Group:flota-devs DeveloperManage Topic flota. PREFIXED
bash /lab/rbac.sh asignar Group:flota-devs DeveloperWrite  Topic flota. PREFIXED
bash /lab/rbac.sh asignar User:bob         DeveloperRead   Topic flota. PREFIXED
bash /lab/rbac.sh asignar User:bob         DeveloperRead   Group analitica PREFIXED

bash /lab/rbac.sh listar Group:flota-devs
bash /lab/rbac.sh listar User:bob
```

Cada llamada debe devolver `HTTP 204`.

## Paso 5 · Probar los permisos

Carol nunca ha recibido un rol de forma individual, pero hereda los del grupo `flota-devs`. Vamos a usarla para crear el topic y producir:

```bash
kafka-topics --bootstrap-server kafka1:19092 --command-config /clientes/carol.properties \
  --create --topic flota.posiciones --partitions 3 --replication-factor 1
bash /scripts/generar-flota.sh 200 kafka1:19092 flota.posiciones /clientes/carol.properties
```

Bob lee con un grupo permitido y falla con uno no permitido, y tampoco puede escribir:

```bash
kafka-console-consumer --bootstrap-server kafka1:19092 --consumer.config /clientes/bob.properties \
  --topic flota.posiciones --group analitica-informes --from-beginning --max-messages 5
kafka-console-consumer --bootstrap-server kafka1:19092 --consumer.config /clientes/bob.properties \
  --topic flota.posiciones --group otro-grupo --from-beginning --max-messages 5
bash /scripts/generar-flota.sh 5 kafka1:19092 flota.posiciones /clientes/bob.properties
```

Y Dave, sin grupo y sin roles, no ve nada:

```bash
kafka-topics --bootstrap-server kafka1:19092 --command-config /clientes/dave.properties --list
```

## Paso 6 · RBAC y ACLs conviven

El autorizador está configurado con `confluent.authorizer.access.rule.providers=CONFLUENT,KRAFT_ACL`, así que también evalúa ACLs. Dale a Dave lectura sobre el topic con una ACL clásica (como superusuario) y comprueba que funciona sin tocar RBAC:

```bash
kafka-acls --bootstrap-server kafka1:19092 --command-config /clientes/superuser.properties \
  --add --allow-principal User:dave --consumer --topic flota.posiciones --group dave-grupo
kafka-console-consumer --bootstrap-server kafka1:19092 --consumer.config /clientes/dave.properties \
  --topic flota.posiciones --group dave-grupo --from-beginning --max-messages 3
```

En un entorno real conviene elegir un mecanismo principal (normalmente RBAC) y reservar las ACLs para casos puntuales, porque depurar permisos repartidos entre dos sistemas es costoso.

## Paso 7 · Logs de auditoría

Con el autorizador de Confluent, los eventos de autorización y de gestión se publican en el topic `confluent-audit-log-events` en formato CloudEvents. Como superusuario, consúmelo y busca los accesos denegados que hemos provocado con Bob y Dave:

```bash
kafka-console-consumer --bootstrap-server kafka1:19092 --consumer.config /clientes/superuser.properties \
  --topic confluent-audit-log-events --from-beginning --timeout-ms 10000 \
  | grep -E '"principal":"User:(bob|dave)"' | head -5
```

Si el topic todavía no existe o no aparece nada, revisa con el formador la configuración de rutas de auditoría (`confluent.security.event.router.config`), que decide qué tipos de evento se registran y en qué topic.

## Paso 8 · Retirar un rol

```bash
bash /lab/rbac.sh quitar User:bob DeveloperRead Group analitica PREFIXED
kafka-console-consumer --bootstrap-server kafka1:19092 --consumer.config /clientes/bob.properties \
  --topic flota.posiciones --group analitica-informes --from-beginning --max-messages 1
```

## Si algo no arranca

Si `kafka1` se detiene al arrancar, mira el final de `docker logs kafka1`. Los fallos más habituales en este laboratorio son tres: no haber generado las claves (el error menciona `tokenKeypair.pem`), que el LDAP no estuviera listo (vuelve a lanzar `docker compose up -d`) o que haya restos de un laboratorio anterior con el mismo nombre de contenedor (`docker compose down -v` en la carpeta del otro laboratorio).

## Preguntas de comprobación

1. ¿Qué ventaja tiene asignar un rol a `Group:flota-devs` frente a asignarlo a cada usuario?
2. ¿Qué diferencia hay entre `ResourceOwner` y `DeveloperManage`?
3. ¿Qué componente de Confluent Platform almacena y sirve los role bindings?
4. ¿Qué valor de `confluent.authorizer.access.rule.providers` permite que convivan RBAC y ACLs en KRaft?
5. ¿En qué topic se escriben por defecto los logs de auditoría?

## Limpieza

```bash
docker compose down -v
```

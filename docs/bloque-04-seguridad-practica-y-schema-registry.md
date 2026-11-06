# Bloque 4 · Seguridad práctica y Schema Registry

**Sesiones 4 y 5 · viernes 6 y martes 10 de noviembre** · Presentación: [`presentaciones/Bloque_04_Seguridad_practica_y_Schema_Registry.pptx`](../presentaciones/Bloque_04_Seguridad_practica_y_Schema_Registry.pptx) · Laboratorios: [Lab 3](../labs/lab03-tls-scram-acl/README.md), [Lab 4](../labs/lab04-rbac-ldap/README.md), [Lab 5](../labs/lab05-schema-registry/README.md)

El bloque pone en práctica la seguridad vista en el Bloque 3 (TLS, SCRAM, ACLs y RBAC) y presenta Schema Registry, la pieza que garantiza que productores y consumidores comparten el mismo contrato de datos.

## 1. Diagnosticar errores de seguridad por capas

Ante un error, lo primero es clasificarlo:

| Síntoma | Capa | Qué revisar |
|---|---|---|
| El cliente no conecta, error de *handshake* | Red o TLS | Puerto y protocolo del listener, truststore, SAN del certificado, `advertised.listeners` |
| `SaslAuthenticationException`, *Authentication failed* | Autenticación | Usuario, contraseña, mecanismo SASL, configuración JAAS |
| `TopicAuthorizationException`, `GroupAuthorizationException` | Autorización | ACLs o role bindings del principal sobre el topic y el grupo |

## 2. Lab 3 · TLS, SASL/SCRAM y ACLs

El clúster del Lab 3 tiene tres listeners: `BROKER` (SSL con autenticación mutua, para tráfico entre brokers y el administrador), `CLIENT` (SASL_SSL con SCRAM-SHA-512, para aplicaciones) y `CONTROLLER` (quórum KRaft).

Pasos clave:

- Generar una CA propia y certificados con `keytool`, incluyendo el nombre del host en el SAN.
- Mapear el DN del certificado del administrador a `User:admin` con `ssl.principal.mapping.rules`.
- Crear usuarios SCRAM: `kafka-configs --alter --entity-type users --entity-name alice --add-config 'SCRAM-SHA-512=[iterations=8192,password=...]'`.
- Conceder permisos con ACLs. Los atajos de `kafka-acls`:

| Atajo | Concede |
|---|---|
| `--producer --topic T` | `WRITE`, `DESCRIBE` y `CREATE` sobre el topic |
| `--consumer --topic T --group G` | `READ` y `DESCRIBE` sobre el topic y `READ` sobre el grupo |

- Usar ACLs con prefijo (`--resource-pattern-type prefixed`) y comprobar que un `DENY` literal anula un `ALLOW` por prefijo.

## 3. Lab 4 · RBAC con LDAP

El Lab 4 conecta un Confluent Server con el **Metadata Service** a un directorio LDAP con usuarios y grupos. Los usuarios se autentican con SASL/PLAIN validado contra LDAP, y los permisos se conceden con *role bindings* a través de la API REST del MDS.

Ideas que se comprueban:

- Un usuario autenticado pero sin roles no ve ni puede hacer nada.
- Un rol asignado a un grupo (`Group:flota-devs`) lo heredan todos sus miembros sin asignaciones individuales.
- Con `confluent.authorizer.access.rule.providers=CONFLUENT,KRAFT_ACL` conviven RBAC y ACLs.
- Las denegaciones quedan registradas en el topic `confluent-audit-log-events`.

## 4. Schema Registry: funcionamiento y compatibilidad

Para el broker, un mensaje son bytes. Schema Registry es un servicio con API REST que guarda los esquemas, les asigna identificadores y comprueba que los cambios son compatibles.

Funcionamiento:

1. El serializador del productor presenta el esquema a Schema Registry, que lo registra si es nuevo y compatible.
2. Schema Registry devuelve un identificador.
3. El productor escribe: byte mágico `0` + identificador del esquema (4 bytes) + datos serializados.
4. El deserializador del consumidor lee el identificador, pide el esquema (una vez, luego lo cachea) y deserializa.

Formatos: **Avro**, **Protobuf** y **JSON Schema**.

Los esquemas se agrupan en **subjects** con versiones sucesivas. La estrategia de nombres por defecto es `TopicNameStrategy` (`<topic>-key` / `<topic>-value`); las alternativas son `RecordNameStrategy` y `TopicRecordNameStrategy`, que permiten varios tipos de evento en un topic.

**Modos de compatibilidad:**

| Modo | Garantiza | Cambios permitidos | Orden de despliegue |
|---|---|---|---|
| `BACKWARD` (por defecto) | El esquema nuevo lee datos escritos con el anterior | Borrar campos; añadir campos opcionales con valor por defecto | Primero consumidores |
| `FORWARD` | El esquema anterior lee datos escritos con el nuevo | Añadir campos; borrar campos opcionales | Primero productores |
| `FULL` | Ambas cosas | Añadir o borrar solo campos con valor por defecto | Indiferente |
| `*_TRANSITIVE` | Lo mismo, contra **todas** las versiones anteriores | — | — |
| `NONE` | Nada | Cualquier cambio | — |

La compatibilidad se fija a nivel global y por subject; la del subject tiene prioridad. Un cambio incompatible se rechaza con HTTP 409.

En Confluent Server se puede activar la **validación de esquemas en el broker** (`confluent.value.schema.validation=true` en el topic), que rechaza mensajes sin un identificador de esquema válido.

## 5. Administración de Schema Registry

- **Estado en Kafka:** todo vive en el topic compactado `_schemas` (una partición; RF 3 en producción). Los nodos lo leen al arrancar y no tienen base de datos propia. Perder ese topic significa perder los esquemas.
- **Alta disponibilidad:** varios nodos con el mismo `schema.registry.group.id` eligen un líder; solo el líder escribe en `_schemas` y los demás le reenvían las escrituras. `leader.eligibility=false` impide que un nodo sea líder.
- **Dos centros:** nodos primarios elegibles en el centro principal y secundarios no elegibles en el de respaldo, para que los identificadores se asignen en un solo sitio. *Schema Linking* replica esquemas entre registros.
- **Modos:** `READWRITE` (normal), `READONLY` (registro secundario, mantenimiento) e `IMPORT` (migración conservando identificadores).
- **Borrado:** primero lógico (`DELETE /subjects/{s}`), que conserva los identificadores; después, si se quiere, físico (`?permanent=true`), irreversible.
- **Seguridad:** TLS en la API, autenticación y RBAC sobre subjects (por ejemplo `DeveloperRead` sobre el prefijo `flota.`).

## Lo que pregunta el examen

- Atajos `--producer` y `--consumer` de `kafka-acls`; clasificación de errores por capa.
- Roles de RBAC y ventaja de asignarlos a grupos; convivencia de RBAC y ACLs.
- Formato del mensaje (identificador, no esquema); estrategia de nombres por defecto.
- `BACKWARD`, `FORWARD`, `FULL`, transitivos, y el orden de despliegue de cada uno.
- Topic `_schemas`; solo el líder escribe; `leader.eligibility`; modos; borrado lógico antes que físico.

## Para practicar

- Simulacro del Bloque 4.
- Labs [3](../labs/lab03-tls-scram-acl/README.md), [4](../labs/lab04-rbac-ldap/README.md) y [5](../labs/lab05-schema-registry/README.md).

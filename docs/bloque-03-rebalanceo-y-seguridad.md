# Bloque 3 · Rebalanceo y seguridad

**Sesión 3 · jueves 5 de noviembre** · Presentación: [`presentaciones/Bloque_03_Rebalanceo_y_seguridad.pptx`](../presentaciones/Bloque_03_Rebalanceo_y_seguridad.pptx) · Laboratorio: [Lab 2](../labs/lab02-escalado-reasignacion/README.md)

La primera parte del bloque trata de qué es un clúster desbalanceado y cómo se corrige, a mano y de forma automática. La segunda construye el modelo de seguridad de Kafka: listeners, cifrado, autenticación, autorización y auditoría.

## 1. Rebalanceo y clústeres desbalanceados

Al añadir un broker, **Kafka no mueve las particiones existentes**: el broker nuevo entra vacío y solo recibe réplicas de topics nuevos o de reasignaciones explícitas.

Tipos de desequilibrio:

- **De réplicas:** unos brokers tienen muchas más particiones que otros.
- **De liderazgo:** las réplicas están repartidas, pero los líderes se concentran en pocos brokers, que reciben todo el tráfico de clientes.
- **De carga:** el número de particiones está equilibrado, pero unas reciben mucho más tráfico que otras.

**Liderazgo.** Kafka reequilibra los líderes hacia la réplica preferida de forma automática:

| Propiedad | Por defecto | Función |
|---|---|---|
| `auto.leader.rebalance.enable` | `true` | Activa el reequilibrado automático |
| `leader.imbalance.check.interval.seconds` | 300 | Cada cuánto se comprueba |
| `leader.imbalance.per.broker.percentage` | 10 | Desequilibrio tolerado por broker |

A demanda: `kafka-leader-election --election-type PREFERRED --all-topic-partitions`. La misma herramienta permite forzar una elección `UNCLEAN` en una partición concreta como último recurso.

**Réplicas: `kafka-reassign-partitions`**, en tres fases:

1. `--generate`: a partir de una lista de topics y de brokers de destino, propone un plan y muestra la asignación actual (guárdala como plan de vuelta atrás).
2. `--execute --throttle <bytes/s>`: aplica el plan limitando el ancho de banda de replicación. Durante la reasignación cada partición tiene a la vez las réplicas antiguas y las nuevas; las antiguas se eliminan cuando las nuevas entran en el ISR.
3. `--verify`: comprueba el estado y, al terminar, **retira el throttle**. Si no se ejecuta, el límite se queda puesto.

`--cancel` cancela una reasignación en curso. El plan generado reparte réplicas de forma uniforme, pero no conoce ni el tamaño ni el tráfico de cada partición.

**Herramientas automáticas:**

| | Self-Balancing Clusters (SBC) | Cruise Control |
|---|---|---|
| Origen | Confluent Server (licencia Enterprise) | Código abierto (LinkedIn) |
| Despliegue | Integrado en los brokers (`confluent.balancer.enable`) | Servicio aparte con API REST |
| Qué hace | Mueve réplicas y líderes según carga real; vacía brokers con `kafka-remove-brokers` | Propone y ejecuta reequilibrios según objetivos priorizados (racks, disco, red, CPU); detecta y repara anomalías |

En Confluent Cloud el reequilibrado lo hace el propio servicio.

## 2. Modelo de seguridad

Por defecto Kafka no tiene seguridad: tráfico en claro, sin autenticación y sin autorización. Las capas son:

| Capa | Pregunta que responde | Fallo típico |
|---|---|---|
| Cifrado en tránsito | ¿Pueden leer o alterar los datos en la red? | Error de *handshake* TLS o cliente que no conecta |
| Autenticación | ¿Quién se conecta? | `SaslAuthenticationException` |
| Autorización | ¿Qué puede hacer? | `TopicAuthorizationException`, `GroupAuthorizationException` |
| Auditoría | ¿Quién hizo qué? | — |
| Cifrado en reposo | ¿Están protegidos los datos en disco? | — |

**Listeners.** Cada listener tiene nombre, puerto y protocolo (`listener.security.protocol.map`):

| Protocolo | Cifrado TLS | Autenticación |
|---|---|---|
| `PLAINTEXT` | No | No |
| `SSL` | Sí | Opcional, con certificado de cliente |
| `SASL_PLAINTEXT` | No | SASL |
| `SASL_SSL` | Sí | SASL |

Es habitual tener varios: uno entre brokers (mTLS), otro para aplicaciones internas y otro para clientes externos.

**TLS.** Cada broker tiene certificado y clave privada en un *keystore*; los clientes confían en la autoridad de certificación mediante un *truststore*. Por defecto (`ssl.endpoint.identification.algorithm=https`) el cliente comprueba que el nombre del host coincide con el certificado, lo que exige un SAN correcto.

**Cifrado en reposo.** No es nativo del broker: se resuelve con cifrado de disco o volumen (sistema operativo o nube) o cifrando en el cliente (Confluent ofrece cifrado de campos en el cliente). En Confluent Cloud los datos se cifran siempre en reposo, con opción de clave propia en clústeres dedicados.

## 3. Autenticación

| Mecanismo | Cómo funciona | Cuándo |
|---|---|---|
| mTLS | Certificado de cliente (`ssl.client.auth=required`); el principal sale del DN, transformable con `ssl.principal.mapping.rules` | Tráfico entre brokers, aplicaciones con PKI propia |
| SASL/PLAIN | Usuario y contraseña enviados tal cual: **solo con TLS** | Simplicidad; en Confluent Platform puede validar contra LDAP |
| SASL/SCRAM-SHA-256/512 | Desafío y respuesta; la contraseña no viaja y se guarda con sal y hash en los metadatos del clúster; se gestiona con `kafka-configs --entity-type users` | Credenciales gestionadas en Kafka |
| SASL/GSSAPI (Kerberos) | Principales y *keytabs* contra un KDC; `sasl.kerberos.principal.to.local.rules` | Organizaciones con Kerberos o Active Directory |
| SASL/OAUTHBEARER | Tokens OAuth 2.0 de un proveedor OIDC | Identidad centralizada (Okta, Entra ID, Keycloak) |

En Confluent Cloud, las aplicaciones se autentican por defecto con API key y secreto mediante SASL/PLAIN sobre TLS.

## 4. Autorización y auditoría

**ACLs.** En KRaft el autorizador es el `StandardAuthorizer` (`authorizer.class.name`). Una ACL tiene: principal, permiso (`ALLOW`/`DENY`), operación (`READ`, `WRITE`, `CREATE`, `DELETE`, `ALTER`, `DESCRIBE`, `ALTER_CONFIGS`, `DESCRIBE_CONFIGS`, `IDEMPOTENT_WRITE`, `ALL`), recurso (topic, grupo, clúster, identificador transaccional, token de delegación), tipo de patrón (`LITERAL` o `PREFIXED`) y host.

Reglas de evaluación:

1. Un `DENY` aplicable **siempre gana** a cualquier `ALLOW`.
2. Sin ACL que lo permita, se deniega (salvo `allow.everyone.if.no.acl.found=true`, no recomendado).
3. Los principales de `super.users` se saltan todas las comprobaciones.
4. `READ`, `WRITE` y `DELETE` sobre un topic implican `DESCRIBE`.
5. Un consumidor con grupo necesita `READ` sobre el topic **y** sobre el grupo.

**RBAC (Confluent Platform).** Se asignan roles predefinidos con un ámbito a usuarios o grupos, que pueden venir de LDAP o Active Directory. Lo almacena y sirve el **Metadata Service (MDS)** de Confluent Server, y es transversal a Kafka, Schema Registry, Connect, ksqlDB y Control Center.

| Ámbito | Roles |
|---|---|
| Clúster | `SystemAdmin`, `ClusterAdmin`, `UserAdmin`, `SecurityAdmin`, `AuditAdmin`, `Operator` |
| Recurso | `ResourceOwner`, `DeveloperManage`, `DeveloperRead`, `DeveloperWrite` |

RBAC y ACLs pueden convivir. Confluent Cloud usa RBAC (por ejemplo `OrganizationAdmin`, `EnvironmentAdmin`, `CloudClusterAdmin`) más ACLs por clúster.

**Auditoría.** Confluent Platform registra decisiones de autorización y operaciones de gestión en formato CloudEvents en el topic `confluent-audit-log-events` (configurable con el enrutador de eventos). Confluent Cloud ofrece logs de auditoría a nivel de organización.

## Lo que pregunta el examen

- Un broker nuevo no recibe réplicas existentes; fases de `kafka-reassign-partitions`; `--verify` retira el throttle.
- Elección de líder preferido y sus propiedades; SBC frente a Cruise Control.
- Los cuatro protocolos; autenticación frente a autorización; TLS valida el host; el cifrado en reposo no es nativo.
- Qué mecanismo SASL encaja en cada escenario; PLAIN exige TLS; SCRAM en los metadatos.
- Componentes de una ACL; `DENY` gana; permiso sobre el grupo; RBAC con MDS y grupos LDAP; topic de auditoría.

## Para practicar

- Simulacro del Bloque 3.
- Lab 2: [`labs/lab02-escalado-reasignacion`](../labs/lab02-escalado-reasignacion/README.md).

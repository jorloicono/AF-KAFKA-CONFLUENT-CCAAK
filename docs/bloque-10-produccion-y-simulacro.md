# Bloque 10 · Producción y simulacro

**Sesiones 12 y 13 · viernes 20 y lunes 23 de noviembre** · Presentación: [`presentaciones/Bloque_10_Produccion_y_simulacro.pptx`](../presentaciones/Bloque_10_Produccion_y_simulacro.pptx) · Laboratorio: [Lab 13](../labs/lab13-terraform-confluent-cloud/README.md)

El último bloque mira a producción: despliegue en Confluent Cloud con Terraform, organización de entornos y gobernanza de datos, migraciones sin parada, y cierre con el simulacro completo del examen y un plan de estudio personal.

## 1. Lab 13 · Confluent Cloud con Terraform

El laboratorio describe en Terraform un entorno con Stream Governance, un clúster Basic, dos topics (`flota.posiciones` y el compactado `flota.vehiculos`) y tres cuentas de servicio: una administradora con `CloudClusterAdmin` y dos aplicaciones con ACLs mínimas (el productor solo escribe en `flota.posiciones`; el consumidor lee los topics `flota.*` con grupos `rutas-*`).

Flujo: leer el código, crear un *workspace* por entorno, `plan` con el fichero de variables del entorno, `apply`, probar los permisos, hacer un cambio (más particiones) leyendo el plan y, al final, **`terraform destroy`** para no generar costes. Al intentar reducir particiones, el plan o el apply lo rechazan, porque Kafka no lo permite.

## 2. Entornos y Stream Governance

**Entornos.** Lo habitual es separar desarrollo, preproducción y producción. La opción más segura es que cada uno tenga sus propios clústeres, su Schema Registry y sus identidades (en Confluent Cloud, un entorno por cada uno). Compartir clúster con prefijos abarata, pero aumenta el riesgo y complica los permisos.

- Entre entornos se promocionan **definiciones**, no datos: topics, esquemas, permisos y conectores, mediante infraestructura como código.
- Las diferencias legítimas (particiones, retención, tipo de clúster) son variables.
- Convención de nombres desde el primer día (por ejemplo `dominio.entidad`).
- Datos de producción en entornos inferiores, solo enmascarados o sintéticos.

**Stream Governance** tiene tres pilares:

| Pilar | Qué aporta |
|---|---|
| Calidad | Schema Registry y **contratos de datos**: esquema más reglas de calidad, metadatos y reglas de migración |
| Catálogo (Stream Catalog) | Buscar topics y esquemas, etiquetarlos (por ejemplo, datos personales) y añadir metadatos de negocio |
| Linaje (Stream Lineage) | Mapa visual de productores, topics, conectores, consultas y consumidores |

En Confluent Cloud se activa por entorno con un paquete (*Essentials* o *Advanced*).

## 3. Migraciones sin parada

**Actualización de versión (rolling upgrade)** en KRaft:

1. Actualizar los controllers uno a uno, comprobando el quórum.
2. Actualizar los brokers uno a uno, esperando a URP = 0 tras cada reinicio.
3. Con todo validado, subir la versión de metadatos con `kafka-features`. Hasta este paso se puede volver atrás; después, no.

Con ZooKeeper, en dos pasadas: binarios nuevos con `inter.broker.protocol.version` en la versión antigua y, después, subir esa propiedad. Los clientes son compatibles en un rango amplio de versiones, aunque Kafka 4.0 retiró versiones de protocolo muy antiguas.

**Migración de ZooKeeper a KRaft:**

1. Estar en la versión puente: Apache Kafka 3.9 o Confluent Platform 7.9.
2. Desplegar los controllers KRaft con la migración activada y conectados a ZooKeeper.
3. Reconfigurar y reiniciar los brokers: los metadatos se copian al log de KRaft y el clúster entra en **escritura dual** (KRaft y ZooKeeper), lo que permite volver atrás.
4. Reiniciar los brokers en modo KRaft puro.
5. Finalizar retirando ZooKeeper de los controllers. A partir de aquí no hay vuelta atrás y se puede actualizar a Kafka 4.0 o CP 8.0.

**Migración entre clústeres con Cluster Linking:**

1. Crear el enlace desde el clúster nuevo y los mirror topics, con sincronización de offsets y ACLs.
2. Esperar a que los mirrors estén al día.
3. Mover los consumidores al clúster nuevo (leen los mirrors con sus offsets sincronizados).
4. Parar los productores en el antiguo y esperar a que el retraso del enlace sea cero.
5. Promover los mirror topics.
6. Arrancar los productores contra el clúster nuevo.

## 4. Repaso: lo que más se falla

1. Durabilidad: RF, `min.insync.replicas` y `acks` a la vez; RF 3, 2 y `all`.
2. Particiones enfermas: under-replicated (funciona), bajo el mínimo del ISR (no acepta `acks=all`, se puede leer), offline (sin líder); `unclean.leader.election.enable`.
3. Quórum KRaft por mayoría, un único controlador activo, `__cluster_metadata`.
4. Precedencia de configuraciones y modos de actualización.
5. Particiones solo aumentan; RF por reasignación; `--verify` retira el throttle.
6. Seguridad por capas; PLAIN con TLS; SCRAM en metadatos; `DENY` gana; permiso sobre el grupo; RBAC con MDS y LDAP.
7. Schema Registry: `_schemas`, identificador en el mensaje, `BACKWARD` por defecto, orden de despliegue, transitivos.
8. Connect: tres topics compactados, `group.id` común, tareas `FAILED` que hay que reiniciar, DLQ solo en sinks.
9. Diagnóstico: lag con clúster sano apunta al consumidor; grupo sin miembros; rebalanceos y sus parámetros; reset con grupo inactivo.
10. Replicación entre clústeres: Cluster Linking conserva nombres y offsets; MM2 renombra y traduce; RPO y RTO.

## 5. Simulacro de examen y plan de estudio

El simulacro final tiene 60 preguntas en 90 minutos, sin documentación. Estrategia: una primera pasada con lo seguro marcando lo dudoso (sin pasar más de dos minutos en una pregunta), una segunda pasada con las marcadas, y no dejar ninguna en blanco. Leer cada pregunta hasta el final: muchas veces la clave está en un detalle.

Confluent no publica la nota de corte. Un resultado por encima del 75–80 % es una buena señal; por debajo, lo importante es en qué dominios se ha fallado.

**Plan de estudio:**

1. Ordenar los dominios de peor a mejor resultado.
2. Para los dos o tres peores, repetir sus laboratorios sin mirar la guía y repasar la documentación oficial de Confluent.
3. Rehacer sus simulacros por bloque hasta acertarlos sin dudar.
4. Fijar fecha de examen en 2–4 semanas y repetir el simulacro final una semana antes.

| Dominio | Bloques | Laboratorios |
|---|---|---|
| Fundamentos de Apache Kafka | 1, 2 | 1 |
| Configuración del clúster | 2, 3, 7 | 1, 2, 9 |
| Seguridad | 3, 4 | 3, 4 |
| Arquitectura de despliegue | 2, 8, 10 | 10, 13 |
| Kafka Connect | 5 | 6 |
| Observabilidad | 5, 6 | 7 |
| Troubleshooting | 6, 7 | 7, 8 |

## Para practicar

- Simulacro del Bloque 10 y simulacro final (`04_Simulacro_final.docx`).
- Lab 13: [`labs/lab13-terraform-confluent-cloud`](../labs/lab13-terraform-confluent-cloud/README.md).

# Formación práctica en Kafka y Confluent · Administración (CCAAK)

Repositorio de laboratorios del curso de 40 horas para administradores de Apache Kafka y Confluent Platform, alineado con la certificación Confluent Certified Administrator for Apache Kafka (CCAAK). Todos los laboratorios comparten un caso práctico: la telemetría de una flota de 20 vehículos de reparto.

## Requisitos del equipo del alumno

Docker Desktop (Windows o macOS) o Docker Engine con el plugin `compose` (Linux), con al menos 6 GB de RAM asignados a Docker (8 GB recomendados para los labs 7, 8 y 12) y 15 GB de disco libre. Salida a Internet para descargar imágenes la primera vez. Un terminal con bash (en Windows, Git Bash o WSL) para los scripts de los labs 8 y 9, y `curl`. Para el Lab 13, Terraform 1.5 o superior y una cuenta de Confluent Cloud.

## Preparación (una vez, antes de la primera sesión)

```bash
# 1. Descargar las imágenes (tarda un rato; mejor hacerlo antes de clase)
docker pull confluentinc/cp-server:7.9.0
docker pull confluentinc/cp-schema-registry:7.9.0
docker pull confluentinc/cp-kafka-connect:7.9.0
docker pull confluentinc/cp-ksqldb-server:7.9.0
docker pull confluentinc/cp-ksqldb-cli:7.9.0
docker pull osixia/openldap:1.5.0
docker pull postgres:16
docker pull prom/prometheus:v2.53.0
docker pull grafana/grafana:11.1.0
docker pull danielqsj/kafka-exporter:v1.8.0
docker pull flink:1.20.1
docker pull alpine:3.20

# 2. Descargar los JAR del agente JMX Exporter y del conector Kafka de Flink
bash comun/scripts/descargar-dependencias.sh
```

## Laboratorios

| Lab | Carpeta | Bloque | Contenido |
|---|---|---|---|
| 1 | `labs/lab01-cluster-kraft` | 2 | Clúster KRaft de 3 brokers, recorrido por la CLI y caída de un broker |
| 2 | `labs/lab02-escalado-reasignacion` | 3 | Añadir un broker, reasignar particiones, configuración dinámica, SBC |
| 3 | `labs/lab03-tls-scram-acl` | 4 | TLS, SASL/SCRAM y ACLs |
| 4 | `labs/lab04-rbac-ldap` | 4 | RBAC con LDAP y Metadata Service, logs de auditoría |
| 5 | `labs/lab05-schema-registry` | 4 | Compatibilidad de esquemas, cambios rechazados, administración |
| 6 | `labs/lab06-connect` | 5 | Connect distribuido PostgreSQL → Kafka, fallos y DLQ |
| 7 | `labs/lab07-prometheus-grafana` | 6 | JMX Exporter, Prometheus, Grafana y alertas |
| 8 | `labs/lab08-simulacro-incidencias` | 7 | Cinco incidencias para diagnosticar |
| 9 | `labs/lab09-rendimiento` | 8 | Pruebas de carga y ajuste de parámetros |
| 10 | `labs/lab10-cluster-linking-mm2` | 8 | Cluster Linking con failover y MirrorMaker 2 |
| 11 | `labs/lab11-ksqldb` | 9 | ksqlDB: streams, tablas, consultas persistentes |
| 12 | `labs/lab12-flink-sql` | 9 | Flink SQL, jobs persistentes y checkpoints |
| 13 | `labs/lab13-terraform-confluent-cloud` | 10 | Terraform con Confluent Cloud |

Cada carpeta tiene su `README.md` con la guía paso a paso y su `docker-compose.yml`. Entre un laboratorio y otro, ejecuta `docker compose down -v` en la carpeta del anterior: varios laboratorios usan los mismos nombres de contenedor y los mismos puertos.

## Versiones

Confluent Platform 7.9.0 (Apache Kafka 3.9) en modo KRaft, que admite tanto las funciones de Apache Kafka como las de Confluent Server (RBAC, Cluster Linking, Self-Balancing Clusters). Las funciones Enterprise funcionan sin licencia durante 30 días desde el primer arranque de cada clúster, que es más que suficiente porque los clústeres del curso se crean y destruyen en cada laboratorio.

## Scripts comunes

`comun/scripts/generar-flota.sh` produce eventos de posición (clave = vehículo) y `comun/scripts/cargar-vehiculos.sh` carga el maestro de vehículos. Se ejecutan dentro de cualquier contenedor con las herramientas de Kafka, por ejemplo `docker exec kafka1 bash /scripts/generar-flota.sh 1000`.


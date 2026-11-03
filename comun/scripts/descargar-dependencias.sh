#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Descarga los JAR que necesitan los laboratorios 7, 8 y 12.
# Ejecutar UNA vez desde la raíz del repositorio (Linux, macOS o Git Bash/WSL):
#
#   bash comun/scripts/descargar-dependencias.sh
#
# En Windows sin bash: descarga manualmente las dos URL de abajo y guárdalas
# en la carpeta comun/descargas/ con el mismo nombre de fichero.
# -----------------------------------------------------------------------------
set -euo pipefail
DEST="$(cd "$(dirname "$0")/.." && pwd)/descargas"
mkdir -p "$DEST"

JMX_VER="1.0.1"
FLINK_KAFKA_VER="3.3.0-1.20"

JMX_URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_VER}/jmx_prometheus_javaagent-${JMX_VER}.jar"
FLINK_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_VER}/flink-sql-connector-kafka-${FLINK_KAFKA_VER}.jar"

echo ">> Descargando agente JMX Exporter ${JMX_VER}"
curl -fL -o "${DEST}/jmx_prometheus_javaagent.jar" "$JMX_URL"

echo ">> Descargando conector Kafka para Flink SQL ${FLINK_KAFKA_VER}"
curl -fL -o "${DEST}/flink-sql-connector-kafka.jar" "$FLINK_URL"

ls -lh "$DEST"
echo ">> Listo"

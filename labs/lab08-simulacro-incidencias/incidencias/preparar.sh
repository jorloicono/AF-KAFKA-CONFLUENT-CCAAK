#!/usr/bin/env bash
# Prepara los topics del simulacro. Ejecutar desde la carpeta del Lab 8 con el entorno arrancado:
#   bash incidencias/preparar.sh
set -euo pipefail
K="docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092"
$K --create --if-not-exists --topic flota.posiciones --partitions 6 --replication-factor 3 --config min.insync.replicas=2
$K --create --if-not-exists --topic pagos --replica-assignment "1:3,3:1,1:3" --config min.insync.replicas=2
$K --create --if-not-exists --topic auditoria --replica-assignment "3"
$K --create --if-not-exists --topic facturas --partitions 3 --replication-factor 3
docker exec kafka1 bash /scripts/generar-flota.sh 2000 > /dev/null 2>&1
docker exec kafka1 bash /scripts/generar-flota.sh 500 kafka1:29092 pagos > /dev/null 2>&1
docker exec kafka1 bash /scripts/generar-flota.sh 500 kafka1:29092 auditoria > /dev/null 2>&1
$K --describe --topic pagos
$K --describe --topic auditoria
echo ">> Entorno del simulacro preparado. Abre Grafana en http://localhost:3000"

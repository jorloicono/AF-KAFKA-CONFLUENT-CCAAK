#!/usr/bin/env bash
# Incidencia 3
set -e
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 --topic facturas \
  --group facturacion --from-beginning --max-messages 1 --timeout-ms 15000 > /dev/null 2>&1 || true
docker exec kafka1 bash /scripts/generar-flota.sh 30000 kafka1:29092 facturas > /dev/null 2>&1
echo ">> Incidencia 3 en marcha. El equipo de facturación dice que 'no les llegan las facturas nuevas'."

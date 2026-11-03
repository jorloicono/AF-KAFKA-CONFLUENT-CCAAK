#!/usr/bin/env bash
# Incidencia 4
set -e
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create --if-not-exists \
  --topic historico.telemetria --replica-assignment "3,3,3" \
  --config retention.ms=-1 --config retention.bytes=-1 --config segment.bytes=52428800 > /dev/null
echo ">> Incidencia 4 en marcha (tarda un minuto en manifestarse)."
docker exec kafka1 kafka-producer-perf-test --topic historico.telemetria --num-records 1200000 \
  --record-size 500 --throughput -1 --producer-props bootstrap.servers=kafka1:29092 acks=1 linger.ms=50 \
  > /dev/null 2>&1 || true
echo ">> El equipo de analítica dice que 'el histórico ha dejado de aceptar datos'."

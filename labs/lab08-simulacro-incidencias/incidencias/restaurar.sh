#!/usr/bin/env bash
# Deja el clúster como al principio del simulacro (después de cada incidencia, si hace falta)
docker unpause kafka3 > /dev/null 2>&1 || true
docker start kafka1 kafka2 kafka3 > /dev/null 2>&1 || true
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --delete --topic historico.telemetria > /dev/null 2>&1 || true
echo ">> Contenedores en marcha. Espera a que no haya particiones under-replicated:"
sleep 20
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --under-replicated-partitions

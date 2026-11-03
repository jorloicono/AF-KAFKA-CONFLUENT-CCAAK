#!/usr/bin/env bash
# Incidencia 5 (dura unos 3 minutos; déjala corriendo en su propia terminal)
docker exec -d kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 --topic flota.posiciones \
  --group rebotes --timeout-ms 240000
echo ">> Incidencia 5 en marcha. El equipo de rutas dice que 'su consumidor va a tirones y a veces se para'."
for i in $(seq 1 25); do
  docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 --topic flota.posiciones \
    --group rebotes --timeout-ms 3000 > /dev/null 2>&1
  sleep 2
done
echo ">> Fin de la incidencia 5"

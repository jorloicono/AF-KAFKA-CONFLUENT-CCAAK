#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Generador de datos de la flota (caso práctico común a todos los laboratorios)
#
# Produce eventos JSON de posición de vehículos con clave = vehiculo_id.
# Se ejecuta DENTRO de un contenedor que tenga las herramientas de Kafka
# (cp-server, cp-kafka, cp-schema-registry...), por ejemplo:
#
#   docker exec kafka1 bash /scripts/generar-flota.sh 1000
#   docker exec kafka1 bash /scripts/generar-flota.sh 0            # modo continuo
#   docker exec cliente bash /scripts/generar-flota.sh 500 kafka1:19092 flota.posiciones /clientes/alice.properties
#
# Parámetros (todos opcionales):
#   $1  número de eventos (0 = continuo, 5 eventos/segundo aprox.)   [1000]
#   $2  bootstrap server                                               [kafka1:29092]
#   $3  topic                                                          [flota.posiciones]
#   $4  fichero de propiedades del productor (seguridad, etc.)         [vacío]
# -----------------------------------------------------------------------------
set -euo pipefail

N="${1:-1000}"
BOOTSTRAP="${2:-kafka1:29092}"
TOPIC="${3:-flota.posiciones}"
PROPS="${4:-}"

VEHICULOS=(V001 V002 V003 V004 V005 V006 V007 V008 V009 V010
           V011 V012 V013 V014 V015 V016 V017 V018 V019 V020)

evento() {
  local i="$1" base_ts="$2"
  local v="${VEHICULOS[$((RANDOM % 20))]}"
  local ts=$((base_ts + i * 200))
  local lat lon vel comb
  lat=$(printf '40.%04d' $((RANDOM % 9999)))
  lon=$(printf -- '-3.%04d' $((RANDOM % 9999)))
  vel=$((40 + RANDOM % 111))          # 40..150 km/h (hay excesos > 120)
  comb=$((5 + RANDOM % 96))           # 5..100 %
  printf '%s|{"vehiculo_id":"%s","ts":%d,"lat":%s,"lon":%s,"velocidad":%d,"combustible":%d}\n' \
         "$v" "$v" "$ts" "$lat" "$lon" "$vel" "$comb"
}

generar() {
  local base_ts
  base_ts=$(date +%s%3N)
  if [[ "$N" == "0" ]]; then
    local i=0
    while true; do
      base_ts=$(date +%s%3N)
      evento 0 "$base_ts"
      i=$((i + 1))
      sleep 0.2
    done
  else
    for ((i = 0; i < N; i++)); do
      evento "$i" "$base_ts"
    done
  fi
}

EXTRA=()
if [[ -n "$PROPS" ]]; then
  EXTRA=(--producer.config "$PROPS")
fi

echo ">> Enviando eventos a ${TOPIC} en ${BOOTSTRAP} (N=${N}; 0 = continuo, Ctrl+C para parar)" >&2
generar | kafka-console-producer \
  --bootstrap-server "$BOOTSTRAP" \
  --topic "$TOPIC" \
  --property parse.key=true \
  --property key.separator='|' \
  "${EXTRA[@]}"
echo ">> Fin del envío" >&2

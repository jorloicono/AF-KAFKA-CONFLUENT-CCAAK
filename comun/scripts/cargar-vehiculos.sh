#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Carga el maestro de vehículos de la flota (20 vehículos) en un topic,
# normalmente compactado (cleanup.policy=compact), con clave = vehiculo_id.
#
#   docker exec kafka1 bash /scripts/cargar-vehiculos.sh
#   docker exec kafka1 bash /scripts/cargar-vehiculos.sh kafka1:29092 flota.vehiculos
# -----------------------------------------------------------------------------
set -euo pipefail
BOOTSTRAP="${1:-kafka1:29092}"
TOPIC="${2:-flota.vehiculos}"
PROPS="${3:-}"

BASES=(Madrid Valencia Sevilla Bilbao)
MODELOS=("Iveco Daily" "Mercedes Sprinter" "Renault Master" "Ford Transit" "Volvo FH")

EXTRA=()
[[ -n "$PROPS" ]] && EXTRA=(--producer.config "$PROPS")

{
  for i in $(seq -w 1 20); do
    v="V0${i}"
    base="${BASES[$((10#$i % 4))]}"
    modelo="${MODELOS[$((10#$i % 5))]}"
    matricula="$((1000 + 10#$i * 37))-K$(printf '%s' "ABCDEFGHJKLMNPRSTVWXYZ" | cut -c$((10#$i))-$((10#$i)))F"
    printf '%s|{"vehiculo_id":"%s","matricula":"%s","modelo":"%s","base":"%s"}\n' \
      "$v" "$v" "$matricula" "$modelo" "$base"
  done
} | kafka-console-producer --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" \
      --property parse.key=true --property key.separator='|' "${EXTRA[@]}"

echo ">> 20 vehículos cargados en ${TOPIC}" >&2

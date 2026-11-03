#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Batería de pruebas de rendimiento del productor y del consumidor.
# Ejecutar dentro de kafka1:   docker exec -it kafka1 bash /lab/pruebas.sh
# Resultados en:               /lab/resultados.csv  (carpeta del laboratorio)
#
# Variables opcionales: REGISTROS (por defecto 200000), THROUGHPUT (-1 = sin límite)
# -----------------------------------------------------------------------------
set -uo pipefail
BS=kafka1:29092
REGISTROS="${REGISTROS:-200000}"
THROUGHPUT="${THROUGHPUT:--1}"
OUT=/lab/resultados.csv
PAYLOAD=/tmp/payload-flota.txt

# Mensajes realistas (JSON de la flota) para que la compresión tenga sentido
if [[ ! -s "$PAYLOAD" ]]; then
  for i in $(seq 1 5000); do
    printf '{"vehiculo_id":"V%03d","ts":%d,"lat":40.%04d,"lon":-3.%04d,"velocidad":%d,"combustible":%d,"estado":"EN_RUTA","conductor":"conductor-%03d"}\n' \
      $((RANDOM % 20 + 1)) $((1730000000000 + i)) $((RANDOM % 9999)) $((RANDOM % 9999)) $((40 + RANDOM % 111)) $((5 + RANDOM % 96)) $((RANDOM % 50))
  done > "$PAYLOAD"
fi

crear_topic() {
  local t="$1" p="$2"
  kafka-topics --bootstrap-server $BS --delete --topic "$t" > /dev/null 2>&1
  sleep 3
  kafka-topics --bootstrap-server $BS --create --topic "$t" --partitions "$p" --replication-factor 3 \
    --config min.insync.replicas=2 > /dev/null
}

echo "prueba,particiones,acks,batch_size,linger_ms,compresion,registros_seg,mb_seg,latencia_media_ms,p99_ms" > "$OUT"

prueba() {
  local nombre="$1" parts="$2" acks="$3" batch="$4" linger="$5" comp="$6"
  local topic="perf.${nombre}"
  crear_topic "$topic" "$parts"
  echo ">> ${nombre}: particiones=${parts} acks=${acks} batch.size=${batch} linger.ms=${linger} compression=${comp}"
  local linea
  linea=$(kafka-producer-perf-test --topic "$topic" --num-records "$REGISTROS" --payload-file "$PAYLOAD" \
      --throughput "$THROUGHPUT" \
      --producer-props bootstrap.servers=$BS acks="$acks" batch.size="$batch" linger.ms="$linger" compression.type="$comp" \
      2>/dev/null | tail -1)
  echo "   $linea"
  # Ejemplo: 200000 records sent, 61349.6 records/sec (9.80 MB/sec), 120.5 ms avg latency, ... 310 ms 99th, ...
  local rps mbs lat p99
  rps=$(echo "$linea" | sed -E 's/.*sent, ([0-9.]+) records\/sec.*/\1/')
  mbs=$(echo "$linea" | sed -E 's/.*\(([0-9.]+) MB\/sec\).*/\1/')
  lat=$(echo "$linea" | sed -E 's/.*MB\/sec\), ([0-9.]+) ms avg latency.*/\1/')
  p99=$(echo "$linea" | sed -E 's/.* ([0-9]+) ms 99th,.*/\1/')
  echo "${nombre},${parts},${acks},${batch},${linger},${comp},${rps},${mbs},${lat},${p99}" >> "$OUT"
}

#        nombre              part acks batch   linger comp
prueba   "base"               6   1    16384   0      none
prueba   "acks0"              6   0    16384   0      none
prueba   "acksall"            6   all  16384   0      none
prueba   "batch-linger"       6   all  131072  20     none
prueba   "lz4"                6   all  131072  20     lz4
prueba   "zstd"               6   all  131072  20     zstd
prueba   "snappy"             6   all  131072  20     snappy
prueba   "gzip"               6   all  131072  20     gzip
prueba   "una-particion"      1   all  131072  20     lz4

echo
echo ">> Consumo del topic perf.lz4 con kafka-consumer-perf-test"
kafka-consumer-perf-test --bootstrap-server $BS --topic perf.lz4 --messages "$REGISTROS" \
  --group perf-consumidor --timeout 60000 2>/dev/null

echo
echo ">> Tamaño en disco de cada topic de prueba (bytes, suma de todas las réplicas)"
kafka-log-dirs --bootstrap-server $BS --describe 2>/dev/null | grep '^{' | \
  tr '{' '\n' | grep -o '"partition":"perf\.[^"]*","size":[0-9]*' | \
  sed -E 's/"partition":"(perf\.[^"]*)-[0-9]+","size":([0-9]+)/\1 \2/' | \
  awk '{s[$1]+=$2} END {for (t in s) printf "%-22s %12d\n", t, s[t]}' | sort

echo
echo ">> Resultados guardados en la carpeta del laboratorio: resultados.csv"
column -s, -t "$OUT" 2>/dev/null || cat "$OUT"

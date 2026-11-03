#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Genera una CA propia y certificados para kafka1, kafka2, kafka3 y el cliente
# administrador (CN=admin). Todo con keytool, que viene en la imagen de Confluent,
# así que no necesitas OpenSSL ni Java en tu equipo. Ejecutar desde esta carpeta:
#
#   docker run --rm --user root -v "$PWD":/lab -w /lab --entrypoint bash \
#     confluentinc/cp-server:7.9.0 generar-certs.sh
#
# (En PowerShell sustituye "$PWD" por "${PWD}")
# -----------------------------------------------------------------------------
set -euo pipefail
PASS=confluent
DIAS=365
mkdir -p secrets
cd secrets
rm -f ./*.jks ./*.crt ./*.csr ./*_creds

echo ">> 1. Autoridad de certificación del curso"
keytool -genkeypair -alias ca -dname "CN=CA-Curso-Kafka,O=Formacion,C=ES" \
  -keystore ca.jks -storepass "$PASS" -keypass "$PASS" \
  -keyalg RSA -keysize 2048 -validity "$DIAS" -ext bc:c
keytool -exportcert -alias ca -keystore ca.jks -storepass "$PASS" -rfc -file ca.crt

echo ">> 2. Truststore común (solo contiene la CA)"
keytool -importcert -noprompt -alias ca -file ca.crt \
  -keystore kafka.truststore.jks -storepass "$PASS"

for NOMBRE in kafka1 kafka2 kafka3 admin; do
  echo ">> 3. Certificado para ${NOMBRE}"
  keytool -genkeypair -alias "$NOMBRE" -dname "CN=${NOMBRE},O=Formacion,C=ES" \
    -keystore "${NOMBRE}.keystore.jks" -storepass "$PASS" -keypass "$PASS" \
    -keyalg RSA -keysize 2048 -validity "$DIAS"
  keytool -certreq -alias "$NOMBRE" -keystore "${NOMBRE}.keystore.jks" \
    -storepass "$PASS" -file "${NOMBRE}.csr"
  keytool -gencert -alias ca -keystore ca.jks -storepass "$PASS" \
    -infile "${NOMBRE}.csr" -outfile "${NOMBRE}.crt" -rfc -validity "$DIAS" \
    -ext "SAN=dns:${NOMBRE},dns:localhost" \
    -ext KU=digitalSignature,keyEncipherment \
    -ext EKU=serverAuth,clientAuth
  # La cadena se importa en orden: primero la CA y después el certificado firmado
  keytool -importcert -noprompt -alias ca -file ca.crt \
    -keystore "${NOMBRE}.keystore.jks" -storepass "$PASS"
  keytool -importcert -noprompt -alias "$NOMBRE" -file "${NOMBRE}.crt" \
    -keystore "${NOMBRE}.keystore.jks" -storepass "$PASS"
done

chmod 644 ./*
echo ">> Certificados generados en $(pwd):"
ls -1

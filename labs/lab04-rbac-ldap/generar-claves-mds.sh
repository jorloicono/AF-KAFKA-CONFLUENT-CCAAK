#!/usr/bin/env sh
# -----------------------------------------------------------------------------
# Genera el par de claves RSA con el que el Metadata Service (MDS) firma los
# tokens. Se ejecuta en un contenedor Alpine para no depender de OpenSSL local:
#
#   docker run --rm -v "$PWD":/lab -w /lab alpine:3.20 sh generar-claves-mds.sh
# -----------------------------------------------------------------------------
set -e
apk add --no-cache -q openssl
mkdir -p secrets
openssl genrsa -traditional -out secrets/tokenKeypair.pem 2048
openssl rsa -in secrets/tokenKeypair.pem -outform PEM -pubout -out secrets/public.pem
chmod 644 secrets/*.pem
ls -l secrets

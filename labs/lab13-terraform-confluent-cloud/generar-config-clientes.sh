#!/usr/bin/env bash
# Genera ficheros de propiedades para probar el productor y el consumidor
# con las API keys que ha creado Terraform. Ejecutar tras "terraform apply".
set -euo pipefail
BOOT=$(terraform output -raw bootstrap_endpoint | sed 's#^SASL_SSL://##')
for APP in productor consumidor; do
  KEY=$(terraform output -raw ${APP}_api_key)
  SECRET=$(terraform output -raw ${APP}_api_secret)
  cat > cliente-${APP}.properties <<PROPS
bootstrap.servers=${BOOT}
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${KEY}" password="${SECRET}";
PROPS
  echo ">> cliente-${APP}.properties generado"
done
echo ">> Bootstrap: ${BOOT}"

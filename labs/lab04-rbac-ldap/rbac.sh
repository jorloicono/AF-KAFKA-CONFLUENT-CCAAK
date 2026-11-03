#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Utilidades para gestionar RBAC a través de la API REST del Metadata Service.
# Se ejecuta dentro del contenedor "cliente":
#
#   bash /lab/rbac.sh id
#   bash /lab/rbac.sh roles
#   bash /lab/rbac.sh asignar  <principal> <rol> <tipo-recurso> <nombre> <LITERAL|PREFIXED>
#   bash /lab/rbac.sh asignar-cluster <principal> <rol>
#   bash /lab/rbac.sh listar   <principal>
#   bash /lab/rbac.sh quitar   <principal> <rol> <tipo-recurso> <nombre> <LITERAL|PREFIXED>
#
# Ejemplo:
#   bash /lab/rbac.sh asignar Group:flota-devs DeveloperWrite Topic flota. PREFIXED
#
# Equivalente con la CLI de Confluent (si la tienes instalada):
#   confluent login --url http://localhost:8090
#   confluent iam rbac role-binding create --principal Group:flota-devs \
#     --role DeveloperWrite --resource Topic:flota. --prefix --kafka-cluster <ID>
# -----------------------------------------------------------------------------
set -euo pipefail
MDS="${MDS:-http://kafka1:8090}"
AUTH="${AUTH:-superuser:superuser-secret}"

cluster_id() {
  curl -s -u "$AUTH" "$MDS/v1/metadata/id" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4
}

scope() {
  printf '{"clusters":{"kafka-cluster":"%s"}}' "$(cluster_id)"
}

cuerpo_binding() {
  local tipo="$1" nombre="$2" patron="$3"
  printf '{"scope":%s,"resourcePatterns":[{"resourceType":"%s","name":"%s","patternType":"%s"}]}' \
    "$(scope)" "$tipo" "$nombre" "$patron"
}

case "${1:-}" in
  id)
    echo "Kafka cluster ID: $(cluster_id)" ;;
  roles)
    curl -s -u "$AUTH" "$MDS/security/1.0/roles" | tr ',' '\n' | grep -o '"name":"[^"]*"' | sort -u ;;
  asignar)
    curl -s -o /dev/null -w "HTTP %{http_code}\n" -u "$AUTH" -X POST -H "Content-Type: application/json" \
      "$MDS/security/1.0/principals/$2/roles/$3/bindings" -d "$(cuerpo_binding "$4" "$5" "$6")" ;;
  asignar-cluster)
    curl -s -o /dev/null -w "HTTP %{http_code}\n" -u "$AUTH" -X POST -H "Content-Type: application/json" \
      "$MDS/security/1.0/principals/$2/roles/$3" -d "$(scope)" ;;
  listar)
    curl -s -u "$AUTH" -X POST -H "Content-Type: application/json" \
      "$MDS/security/1.0/lookup/principal/$2/resources" -d "$(scope)"; echo ;;
  quitar)
    curl -s -o /dev/null -w "HTTP %{http_code}\n" -u "$AUTH" -X DELETE -H "Content-Type: application/json" \
      "$MDS/security/1.0/principals/$2/roles/$3/bindings" -d "$(cuerpo_binding "$4" "$5" "$6")" ;;
  *)
    sed -n '2,22p' "$0" ;;
esac

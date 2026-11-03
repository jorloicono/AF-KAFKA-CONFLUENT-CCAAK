#!/usr/bin/env bash
# Incidencia 2
docker pause kafka3 > /dev/null && echo ">> Incidencia 2 en marcha. Espera 30 segundos e intenta producir en 'pagos' y leer de 'auditoria'."

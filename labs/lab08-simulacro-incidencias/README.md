# Lab 8 · Simulacro de incidencias: romper el clúster de cinco maneras y diagnosticarlo

**Bloque 7 · Duración estimada: 90–105 minutos**

## Objetivo

Este laboratorio es un ensayo de guardia. El formador (o tu compañero de pareja) va a provocar cinco incidencias en el clúster sin decirte cuál es cada una, y tu trabajo es diagnosticarla usando solo lo que tendrías en producción: el cuadro de mando de Grafana, las alertas de Prometheus, las herramientas de línea de comandos y los logs de los brokers. Para cada incidencia rellenarás una hoja de diagnóstico con el síntoma, las evidencias, la causa raíz, la acción correctora y cómo has validado que el clúster ha vuelto a estar sano. El examen CCAAK dedica una parte importante de sus preguntas exactamente a este razonamiento.

## Punto de partida

El entorno es el mismo del Lab 7 (tres brokers con JMX Exporter, `kafka-exporter`, Prometheus y Grafana), con una diferencia: el broker 3 guarda sus datos en un disco pequeño de 400 MB. Los scripts de las incidencias son de bash; en Windows ejecútalos desde Git Bash o WSL.

```bash
cd labs/lab08-simulacro-incidencias
docker compose up -d
sleep 30
bash incidencias/preparar.sh
```

Abre Grafana (http://localhost:3000), la página de alertas de Prometheus (http://localhost:9090/alerts) y deja una terminal con el productor de la flota en marcha durante todo el simulacro:

```bash
docker exec -it kafka1 bash /scripts/generar-flota.sh 0
```

## Cómo se juega

El formador lanzará `bash incidencias/incidencia-N.sh` en su equipo de demostración o, si trabajáis en parejas, uno de los dos lo lanza sin enseñar la pantalla. A partir de ese momento tenéis 12 minutos por incidencia. Al terminar, se ejecuta `bash incidencias/restaurar.sh` y se pone en común el diagnóstico antes de pasar a la siguiente. No leas el contenido de los scripts antes de diagnosticar: el objetivo es llegar a la causa desde los síntomas.

## Caja de herramientas

Todos estos comandos se lanzan con `docker exec kafka1 ...` (o desde una shell dentro de `kafka1`, o de cualquier broker que siga vivo):

```bash
# Estado general de topics y particiones
kafka-topics --bootstrap-server kafka1:29092 --describe --under-replicated-partitions
kafka-topics --bootstrap-server kafka1:29092 --describe --under-min-isr-partitions
kafka-topics --bootstrap-server kafka1:29092 --describe --unavailable-partitions
kafka-topics --bootstrap-server kafka1:29092 --describe --topic <topic>

# Quórum KRaft y brokers registrados
kafka-metadata-quorum --bootstrap-server kafka1:29092 describe --status
kafka-metadata-quorum --bootstrap-server kafka1:29092 describe --replication
kafka-broker-api-versions --bootstrap-server kafka1:29092 | grep "id:"

# Consumidores
kafka-consumer-groups --bootstrap-server kafka1:29092 --list
kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group <grupo>
kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group <grupo> --state
kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group <grupo> --members

# Disco
kafka-log-dirs --bootstrap-server kafka1:29092 --describe --broker-list 1,2,3

# Probar a escribir con la máxima durabilidad
echo "prueba" | kafka-console-producer --bootstrap-server kafka1:29092 --topic <topic> \
  --producer-property acks=all --producer-property delivery.timeout.ms=15000 --producer-property request.timeout.ms=10000

# Logs (desde tu terminal)
docker compose ps -a
docker logs --tail 100 kafka3
docker exec kafka3 df -h /var/lib/kafka/data
```

## Hoja de diagnóstico

Copia esta tabla una vez por incidencia y rellénala.

| Campo | Tu respuesta |
|---|---|
| Síntoma observado (lo que diría el usuario) | |
| Evidencias en Grafana y alertas | |
| Evidencias en la CLI | |
| Evidencias en logs | |
| Causa raíz | |
| Impacto (¿se pierden datos?, ¿quién está afectado?) | |
| Acción correctora | |
| Validación (cómo sé que está resuelto) | |
| Prevención (qué cambiaría en configuración o monitorización) | |

## Pistas, solo si os atascáis

Para la primera, mirad cuántos brokers están vivos y quién lidera cada partición. Para la segunda, probad a escribir con `acks=all` en `pagos` y a leer de `auditoria`, y comparad lo que veis en `--under-min-isr-partitions` y en `--unavailable-partitions`. Para la tercera, mirad la columna `CONSUMER-ID` y el estado del grupo. Para la cuarta, los logs del broker afectado dicen la verdad, y el espacio en disco también. Para la quinta, ejecutad varias veces seguidas el `--describe --state` del grupo y fijaos en el estado y en el número de miembros.

## Validación final

Cuando terminéis las cinco, ejecutad `bash incidencias/restaurar.sh` y comprobad que el panel vuelve a mostrar 3 brokers, 1 controlador y ceros en todos los indicadores rojos.

## Limpieza

```bash
docker compose down -v
```

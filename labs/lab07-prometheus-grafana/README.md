# Lab 7 · Prometheus y Grafana: cuadro de mando con las métricas clave del clúster

**Bloque 6 · Duración estimada: 75 minutos**

## Objetivo

Kafka expone cientos de métricas por JMX, pero un administrador vive de una docena. En este laboratorio vamos a exportar esas métricas a Prometheus con el agente JMX Exporter, a medir el lag de los consumidores con `kafka-exporter`, a cargar un cuadro de mando de Grafana preparado para el curso y a ver cómo se encienden las alertas cuando el clúster sufre. Este mismo entorno es el que usaremos en el Lab 8 para el simulacro de incidencias, así que conviene que salgas de aquí sabiendo leer cada panel.

## Punto de partida

Si no lo hiciste al preparar el curso, descarga el agente JMX Exporter desde la raíz del repositorio y arranca el entorno:

```bash
bash comun/scripts/descargar-dependencias.sh
cd labs/lab07-prometheus-grafana
docker compose up -d
docker compose ps
```

Comprueba que cada broker publica sus métricas en el puerto 7071 y que Prometheus las está recogiendo:

```bash
docker exec kafka1 curl -s localhost:7071/metrics | grep -E "^kafka_server_replicamanager_(underreplicated|leadercount)"
```

Abre http://localhost:9090/targets: los tres brokers y `kafka-exporter` deben aparecer en estado `UP`. Después abre http://localhost:3000 (usuario `admin`, contraseña `admin`, aunque el acceso anónimo ya tiene permisos de administración) y entra en el panel **Kafka · Curso CCAAK**, dentro de la carpeta *Kafka*.

## Paso 1 · Recorrido por el panel en reposo

Antes de generar carga, lee el panel de arriba abajo. La fila de salud debe mostrar 3 brokers activos, exactamente 1 controlador activo y ceros en particiones under-replicated, bajo min ISR, offline y directorios offline. Si alguno de esos números no es el esperado en un clúster sano, ese es el primer sitio al que mirar en cualquier incidencia. Las métricas de controlador vienen del nodo que en ese momento es el líder del quórum KRaft; por eso la suma debe ser siempre 1.

## Paso 2 · Generar carga

Vamos a trabajar con tres terminales. En la primera, crea el topic y deja corriendo el productor continuo de la flota:

```bash
docker exec kafka1 kafka-topics --bootstrap-server kafka1:29092 --create \
  --topic flota.posiciones --partitions 6 --replication-factor 3
docker exec -it kafka1 bash /scripts/generar-flota.sh 0
```

En la segunda, deja corriendo un consumidor con grupo:

```bash
docker exec -it kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 \
  --topic flota.posiciones --group panel-flota > /dev/null
```

En la tercera, lanza una prueba de carga corta para ver un pico de tráfico de verdad:

```bash
docker exec -it kafka1 kafka-producer-perf-test --topic flota.posiciones --num-records 300000 \
  --record-size 512 --throughput 20000 --producer-props bootstrap.servers=kafka1:29092 acks=all
```

En Grafana deberías ver subir los bytes y mensajes de entrada, repartidos entre los tres brokers, y moverse la latencia p99 de `Produce`. Fíjate en que los bytes de salida son mayores que los de entrada: además de los consumidores, los followers también leen del líder para replicar (en este panel la replicación entra en `BytesOutPerSec` del líder).

## Paso 3 · Lag de consumidores y primera alerta

Detén el consumidor de la segunda terminal con `Ctrl+C`, deja el productor en marcha y consulta el grupo desde la tercera:

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 --describe --group panel-flota
```

El lag crece de forma lineal porque nadie consume, y el panel de lag lo refleja. Cuando supere 1.000 mensajes durante un minuto, la alerta `LagConsumidorAlto` pasará de *pending* a *firing* en http://localhost:9090/alerts (al ritmo del generador, unos cuatro minutos; si tienes prisa, repite la prueba de carga del paso anterior). Vuelve a lanzar el consumidor en la segunda terminal y observa cómo el lag se recupera.

## Paso 4 · Un broker caído en el panel

Desde tu terminal, fuera del contenedor:

```bash
docker stop kafka3
```

En menos de un minuto verás bajar los brokers activos a 2, subir las particiones under-replicated, un pico en las contracciones de ISR y, en Prometheus, las alertas `BrokerCaido` y `ParticionesUnderReplicated`. Las particiones bajo min ISR deberían seguir en cero, porque con `min.insync.replicas=2` y dos réplicas vivas se sigue cumpliendo el mínimo. Arranca de nuevo el broker y observa las expansiones de ISR mientras se pone al día:

```bash
docker start kafka3
```

## Paso 5 · Practicar PromQL

En la pestaña *Graph* de Prometheus (http://localhost:9090/graph) escribe tú las consultas que responden a estas preguntas. Las soluciones están al final.

1. ¿Cuántas particiones lidera cada broker?
2. ¿Qué broker recibe más bytes por segundo en los últimos 5 minutos?
3. ¿Cuál es la latencia p99 de las peticiones de fetch de los followers?
4. ¿Cuánto ocupa en disco el topic `flota.posiciones` en cada broker?

## Paso 6 · Añade un panel

Añade al cuadro de mando un panel de series temporales con el tamaño de la cola de peticiones de cada broker (`kafka_network_requestchannel_requestqueuesize`). Una cola que crece de forma sostenida, junto con un porcentaje de hilos de E/S ociosos cercano a cero, indica que el broker no da abasto con las peticiones que recibe.

## Y en Confluent Platform…

Lo que hemos montado aquí es la opción abierta. En Confluent Platform, Control Center ofrece paneles equivalentes de forma integrada (además de gestión de topics, conectores y consultas), y Health+ envía telemetría a Confluent para recibir alertas y recomendaciones gestionadas. En Confluent Cloud, las métricas se consultan con la Metrics API, que también se puede integrar en Prometheus.

## Soluciones de PromQL

```promql
sum by (instance) (kafka_server_replicamanager_leadercount)
topk(1, sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesinpersec_total[5m])))
max by (instance) (kafka_network_requestmetrics_totaltimems{request="FetchFollower", quantile="0.99"})
sum by (instance) (kafka_log_log_size{topic="flota.posiciones"})
```

## Preguntas de comprobación

1. ¿Qué tres métricas mirarías primero para saber si el clúster está sano?
2. ¿Por qué `ActiveControllerCount` debe sumar exactamente 1 en todo el clúster?
3. ¿Qué diferencia hay entre `UnderReplicatedPartitions` y `UnderMinIsrPartitionCount`, y cuál afecta directamente a los productores con `acks=all`?
4. ¿Qué indica un `NetworkProcessorAvgIdlePercent` cercano a cero?
5. ¿De dónde sale el lag de un grupo de consumidores?

## Limpieza

Si vas a hacer el Lab 8 a continuación, no hace falta conservar nada de este entorno:

```bash
docker compose down -v
```

# Lab 9 · Pruebas de carga con kafka-producer-perf-test y ajuste de parámetros

**Bloque 8 · Duración estimada: 60 minutos**

## Objetivo

Cuando alguien pregunta "¿cuánto aguanta nuestro Kafka?", la única respuesta honesta es medirlo. En este laboratorio vamos a usar las herramientas de prueba de carga que vienen con Kafka para cuantificar el efecto de los parámetros que el examen relaciona con rendimiento y durabilidad: `acks`, `batch.size`, `linger.ms`, el tipo de compresión y el número de particiones. Al final tendrás una tabla con tus propios números y sabrás defender con datos por qué se recomiendan ciertas configuraciones.

## Punto de partida

```bash
cd labs/lab09-rendimiento
docker compose up -d
sleep 20
docker exec -it kafka1 bash
```

## Paso 1 · Una prueba a mano

Antes de lanzar la batería completa, entiende la herramienta con una prueba sencilla:

```bash
kafka-topics --bootstrap-server kafka1:29092 --create --topic perf.manual --partitions 6 --replication-factor 3
kafka-producer-perf-test --topic perf.manual --num-records 200000 --record-size 500 --throughput -1 \
  --producer-props bootstrap.servers=kafka1:29092 acks=all
```

La herramienta imprime cada pocos segundos el ritmo alcanzado y, al final, una línea de resumen con registros por segundo, MB por segundo, latencia media y máxima, y los percentiles 50, 95, 99 y 99,9. Con `--throughput -1` no se limita el ritmo; si pones un número, el productor se frena para no superarlo, lo que sirve para medir la latencia con una carga constante y realista.

Ahora mide el consumo:

```bash
kafka-consumer-perf-test --bootstrap-server kafka1:29092 --topic perf.manual --messages 200000 --group perf-manual
```

## Paso 2 · La batería de pruebas

El script `pruebas.sh` ejecuta nueve pruebas con los mismos mensajes JSON de la flota (realistas, para que la compresión tenga efecto), guarda los resultados en `resultados.csv` y al final muestra el espacio que ocupa cada topic en disco:

```bash
bash /lab/pruebas.sh
```

Tarda entre cinco y diez minutos según tu equipo. Mientras corre, anticipa en voz alta qué esperas de cada prueba. Si tu equipo es lento, reduce el volumen con `REGISTROS=100000 bash /lab/pruebas.sh`.

## Paso 3 · Analizar los resultados

Abre `plantilla-analisis.md` en la carpeta del laboratorio, completa la tabla con tus datos y responde a las preguntas. Recuerda que en un portátil con tres brokers en contenedores, compartiendo disco y CPU, los números absolutos no significan nada; lo que importa son las diferencias relativas entre pruebas. Estas son las tendencias que normalmente se observan y que conviene que contrastes con tus datos:

`acks=0` da el mayor rendimiento porque el productor no espera confirmación, a costa de no saber si el mensaje ha llegado. `acks=1` espera solo al líder y `acks=all` espera a todas las réplicas del ISR, lo que añade latencia pero garantiza que el mensaje sobrevive a la caída del líder. Subir `batch.size` y dar algo de margen con `linger.ms` permite agrupar más mensajes por petición, lo que suele mejorar el rendimiento de forma notable a cambio de unos milisegundos de latencia. La compresión reduce los bytes por la red y en disco a cambio de CPU en el productor: con datos JSON, `zstd` suele comprimir más, `lz4` y `snappy` son más rápidos, y `gzip` comprime bien pero cuesta más CPU. Y un topic con una sola partición tiene un único líder y un único hilo de escritura en un único broker, así que no puede aprovechar el paralelismo del clúster.

## Paso 4 · Del laboratorio al dimensionamiento

Con los datos en la mano, haz este cálculo para la quinta pregunta de la plantilla. Una entrada sostenida de 50 MB/s son unos 4,3 TB al día; con 7 días de retención son unos 30 TB; con factor de replicación 3, unos 90 TB, a los que hay que sumar un margen de seguridad de al menos un 30 o 40 % para picos, reasignaciones y para no trabajar con los discos llenos. Si se usa compresión, el tamaño en disco es el de los datos comprimidos, que es lo que has medido en el paso anterior.

## Para profundizar

Si te sobra tiempo, repite la prueba `acksall` con `--throughput 5000` para medir la latencia a carga constante, o con `max.in.flight.requests.per.connection=1` y compara. También puedes lanzar una prueba con un broker detenido (`docker stop kafka3` desde fuera) para ver el efecto de perder un tercio del clúster.

## Preguntas de comprobación

1. ¿Qué parámetro del productor tiene más efecto sobre la durabilidad y cuál sobre el tamaño de los lotes?
2. ¿Por qué `linger.ms=0` no significa que cada mensaje se envíe por separado?
3. ¿En qué componente se paga el coste de CPU de la compresión, y en cuál se descomprime?
4. ¿Qué limita el paralelismo de consumo de un topic?

## Limpieza

```bash
docker compose down -v
```

# Análisis de resultados · Lab 9

Completa esta tabla con los datos de `resultados.csv` y responde a las preguntas.

| Prueba | Registros/s | MB/s | Latencia media (ms) | p99 (ms) | Tamaño en disco |
|---|---|---|---|---|---|
| base (acks=1) | | | | | |
| acks0 | | | | | |
| acksall | | | | | |
| batch-linger | | | | | |
| lz4 | | | | | |
| zstd | | | | | |
| snappy | | | | | |
| gzip | | | | | |
| una-particion | | | | | |

1. ¿Cuánto rendimiento has perdido al pasar de `acks=1` a `acks=all` y qué has ganado a cambio?
2. ¿Qué efecto han tenido `batch.size` y `linger.ms` sobre el rendimiento y sobre la latencia?
3. ¿Qué algoritmo de compresión ha dado mejor relación entre rendimiento y espacio en disco con estos datos?
4. ¿Por qué el topic de una sola partición rinde menos con la misma configuración del productor?
5. Si un cliente te pide 50 MB/s de entrada con retención de 7 días y RF=3, ¿cuánto disco necesitarías en total?

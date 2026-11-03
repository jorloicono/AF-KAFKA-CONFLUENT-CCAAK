# Lab 13 · Crear entorno, clúster, topics y permisos en Confluent Cloud con Terraform

**Bloque 10 · Duración estimada: 60–75 minutos**

## Objetivo

En producción nadie debería crear topics ni conceder permisos a mano desde una consola: si no está en código, no está bajo control de cambios, no se puede revisar y no se puede repetir en otro entorno. En este laboratorio describimos con Terraform toda la infraestructura de la flota en Confluent Cloud (un entorno con Stream Governance, un clúster, dos topics, tres cuentas de servicio con sus API keys y las ACLs mínimas que necesita cada aplicación) y la desplegamos en un entorno de desarrollo. Después la modificamos como se haría con un cambio real y, al final, lo destruimos todo para no generar costes.

## Requisitos y aviso de costes

Necesitas una cuenta de Confluent Cloud (el alta incluye crédito de prueba), Terraform 1.5 o superior y una *Cloud API Key* con permisos de administración de la organización (en la consola: *Administration → API keys → Add key → My account* o una cuenta de servicio con rol `OrganizationAdmin`). El clúster Basic que creamos tiene un coste muy bajo y se cubre con el crédito de prueba, pero **es imprescindible ejecutar `terraform destroy` al terminar**. Si el cliente no permite usar cuentas personales de Confluent Cloud, este laboratorio lo hará el formador como demostración y los alumnos seguirán el código y el plan.

## Punto de partida

```bash
cd labs/lab13-terraform-confluent-cloud
export CONFLUENT_CLOUD_API_KEY="<tu-cloud-api-key>"
export CONFLUENT_CLOUD_API_SECRET="<tu-cloud-api-secret>"
terraform init
```

En PowerShell: `$env:CONFLUENT_CLOUD_API_KEY="..."` y `$env:CONFLUENT_CLOUD_API_SECRET="..."`.

## Paso 1 · Leer el código antes de ejecutarlo

Abre `main.tf` y recórrelo de arriba abajo. Fíjate en tres patrones que se repiten en cualquier proyecto de Terraform con Confluent Cloud.

El primero es la separación entre la API key de Cloud (la de tu cuenta, que crea entornos, clústeres y cuentas de servicio) y las API keys de Kafka (asociadas a un clúster concreto, que es lo que usan las aplicaciones y también Terraform para crear topics y ACLs a través del `rest_endpoint` del clúster).

El segundo es el principio de mínimo privilegio: la cuenta `app-manager` tiene el rol `CloudClusterAdmin` para gestionar el clúster, pero las aplicaciones solo reciben ACLs concretas. El productor puede escribir en `flota.posiciones` y nada más; el consumidor puede leer los topics que empiezan por `flota.` usando grupos que empiecen por `rutas-`.

El tercero es la parametrización por entorno: las diferencias entre desarrollo, preproducción y producción (particiones, retención) están en `entornos/*.tfvars`, y el código es el mismo para los tres.

## Paso 2 · Workspace y plan

Usamos un *workspace* de Terraform por entorno para que cada uno tenga su propio estado:

```bash
terraform workspace new dev
terraform plan -var-file=entornos/dev.tfvars -out=dev.plan
```

Lee el plan: deben aparecer unos quince recursos para crear y ninguno para modificar o destruir. Revisar el plan es el paso que en un equipo real se hace en la *pull request*, antes de aplicar.

## Paso 3 · Aplicar

```bash
terraform apply dev.plan
terraform output
```

La creación del clúster tarda unos minutos. Al terminar, entra en la consola de Confluent Cloud y comprueba que existe el entorno `formacion-flota-dev` con su clúster, los dos topics y las tres cuentas de servicio. Los secretos de las API keys no se muestran en la salida porque están marcados como `sensitive`, pero quedan en el fichero de estado: por eso el estado de Terraform nunca se sube a un repositorio y en equipos se guarda en un backend remoto cifrado.

## Paso 4 · Probar los permisos

Genera los ficheros de configuración de los clientes con las API keys creadas y pruébalos desde un contenedor con las herramientas de Kafka:

```bash
bash generar-config-clientes.sh

docker run --rm -it -v "$PWD":/lab --entrypoint bash confluentinc/cp-server:7.9.0
```

Dentro del contenedor (sustituye `<bootstrap>` por el valor que ha mostrado el script):

```bash
echo 'V001|{"vehiculo_id":"V001","velocidad":90}' | kafka-console-producer --bootstrap-server <bootstrap> \
  --producer.config /lab/cliente-productor.properties --topic flota.posiciones \
  --property parse.key=true --property key.separator='|'

kafka-console-consumer --bootstrap-server <bootstrap> --consumer.config /lab/cliente-consumidor.properties \
  --topic flota.posiciones --group rutas-lab --from-beginning --max-messages 1

kafka-console-consumer --bootstrap-server <bootstrap> --consumer.config /lab/cliente-consumidor.properties \
  --topic flota.posiciones --group otro-grupo --from-beginning --max-messages 1

echo 'x' | kafka-console-producer --bootstrap-server <bootstrap> \
  --producer.config /lab/cliente-consumidor.properties --topic flota.posiciones
```

Las dos primeras operaciones funcionan; la tercera falla por el grupo y la cuarta porque el consumidor no tiene permiso de escritura. Son los mismos errores de autorización del Lab 3, pero ahora con los permisos definidos como código.

## Paso 5 · Un cambio como los de verdad

Supón que el equipo pide más particiones para `flota.posiciones` en desarrollo. Cambia `particiones_posiciones` a 6 en `entornos/dev.tfvars` y ejecuta:

```bash
terraform plan -var-file=entornos/dev.tfvars
```

El plan muestra una modificación *in place* del topic (`~`), no una destrucción. Prueba ahora a bajar el valor a 2 y vuelve a planificar: el proveedor no puede reducir particiones (Kafka no lo permite) y te lo indicará en el plan o al aplicar. Es un buen ejemplo de por qué el plan se lee siempre antes de aplicar. Deja el valor en 6 y aplica:

```bash
terraform apply -var-file=entornos/dev.tfvars
```

## Paso 6 · Otros entornos (solo plan)

Para ver cómo se desplegaría preproducción sin crear nada:

```bash
terraform workspace new pre
terraform plan -var-file=entornos/pre.tfvars
terraform workspace select dev
```

En un proyecto real, cada entorno suele vivir además en su propio entorno de Confluent Cloud (como aquí) con su propio paquete de Stream Governance, sus propias cuentas de servicio y, en producción, un tipo de clúster con alta disponibilidad multizona (`Standard` o `Dedicated`).

## Paso 7 · Destruir

```bash
terraform workspace select dev
terraform destroy -var-file=entornos/dev.tfvars
```

Comprueba en la consola que el entorno ha desaparecido. Si creaste algo en el workspace `pre`, destrúyelo también.

## Preguntas de comprobación

1. ¿Qué diferencia hay entre una Cloud API Key y una API key de Kafka?
2. ¿Por qué el productor no recibe el rol `CloudClusterAdmin`?
3. ¿Qué ventaja aporta separar los entornos con workspaces y ficheros `tfvars`?
4. ¿Dónde quedan guardados los secretos de las API keys y qué implica para el estado de Terraform?
5. ¿Qué paquete de Stream Governance hemos activado y qué incluye?

## Limpieza

Ya realizada en el paso 7. Revoca también la Cloud API Key si la creaste solo para el curso.

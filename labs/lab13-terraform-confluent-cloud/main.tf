# =============================================================================
# Lab 13 · Entorno, clúster, topics y permisos en Confluent Cloud con Terraform
# =============================================================================

locals {
  prefijo = "formacion-flota-${var.entorno}"
}

# --- Entorno con Stream Governance ------------------------------------------
resource "confluent_environment" "flota" {
  display_name = local.prefijo

  stream_governance {
    package = "ESSENTIALS"
  }
}

# --- Clúster Basic (el más económico; suficiente para el laboratorio) ---------
resource "confluent_kafka_cluster" "flota" {
  display_name = "${local.prefijo}-cluster"
  availability = "SINGLE_ZONE"
  cloud        = var.cloud
  region       = var.region
  basic {}

  environment {
    id = confluent_environment.flota.id
  }
}

# --- Cuenta de servicio administradora (gestiona topics y ACLs) ---------------
resource "confluent_service_account" "app_manager" {
  display_name = "${local.prefijo}-app-manager"
  description  = "Administra topics y ACLs del clúster de la flota (${var.entorno})"
}

resource "confluent_role_binding" "app_manager_cluster_admin" {
  principal   = "User:${confluent_service_account.app_manager.id}"
  role_name   = "CloudClusterAdmin"
  crn_pattern = confluent_kafka_cluster.flota.rbac_crn
}

resource "confluent_api_key" "app_manager" {
  display_name = "${local.prefijo}-app-manager-key"
  description  = "API key de Kafka de la cuenta app-manager"

  owner {
    id          = confluent_service_account.app_manager.id
    api_version = confluent_service_account.app_manager.api_version
    kind        = confluent_service_account.app_manager.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.flota.id
    api_version = confluent_kafka_cluster.flota.api_version
    kind        = confluent_kafka_cluster.flota.kind
    environment {
      id = confluent_environment.flota.id
    }
  }

  depends_on = [confluent_role_binding.app_manager_cluster_admin]
}

# --- Topics -----------------------------------------------------------------
resource "confluent_kafka_topic" "posiciones" {
  kafka_cluster {
    id = confluent_kafka_cluster.flota.id
  }
  topic_name       = "flota.posiciones"
  partitions_count = var.particiones_posiciones
  rest_endpoint    = confluent_kafka_cluster.flota.rest_endpoint
  config = {
    "cleanup.policy" = "delete"
    "retention.ms"   = tostring(var.retencion_posiciones_ms)
  }
  credentials {
    key    = confluent_api_key.app_manager.id
    secret = confluent_api_key.app_manager.secret
  }
}

resource "confluent_kafka_topic" "vehiculos" {
  kafka_cluster {
    id = confluent_kafka_cluster.flota.id
  }
  topic_name       = "flota.vehiculos"
  partitions_count = 3
  rest_endpoint    = confluent_kafka_cluster.flota.rest_endpoint
  config = {
    "cleanup.policy" = "compact"
  }
  credentials {
    key    = confluent_api_key.app_manager.id
    secret = confluent_api_key.app_manager.secret
  }
}

# --- Aplicación productora: solo puede escribir en flota.posiciones ---------
resource "confluent_service_account" "productor" {
  display_name = "${local.prefijo}-productor"
  description  = "Telemetría de los vehículos (${var.entorno})"
}

resource "confluent_api_key" "productor" {
  display_name = "${local.prefijo}-productor-key"
  description  = "API key de Kafka del productor"
  owner {
    id          = confluent_service_account.productor.id
    api_version = confluent_service_account.productor.api_version
    kind        = confluent_service_account.productor.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.flota.id
    api_version = confluent_kafka_cluster.flota.api_version
    kind        = confluent_kafka_cluster.flota.kind
    environment {
      id = confluent_environment.flota.id
    }
  }
}

resource "confluent_kafka_acl" "productor_write" {
  kafka_cluster {
    id = confluent_kafka_cluster.flota.id
  }
  resource_type = "TOPIC"
  resource_name = confluent_kafka_topic.posiciones.topic_name
  pattern_type  = "LITERAL"
  principal     = "User:${confluent_service_account.productor.id}"
  host          = "*"
  operation     = "WRITE"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.flota.rest_endpoint
  credentials {
    key    = confluent_api_key.app_manager.id
    secret = confluent_api_key.app_manager.secret
  }
}

# --- Aplicación consumidora: lee flota.* con grupos que empiecen por "rutas-" -
resource "confluent_service_account" "consumidor" {
  display_name = "${local.prefijo}-consumidor"
  description  = "Servicio de rutas (${var.entorno})"
}

resource "confluent_api_key" "consumidor" {
  display_name = "${local.prefijo}-consumidor-key"
  description  = "API key de Kafka del consumidor"
  owner {
    id          = confluent_service_account.consumidor.id
    api_version = confluent_service_account.consumidor.api_version
    kind        = confluent_service_account.consumidor.kind
  }
  managed_resource {
    id          = confluent_kafka_cluster.flota.id
    api_version = confluent_kafka_cluster.flota.api_version
    kind        = confluent_kafka_cluster.flota.kind
    environment {
      id = confluent_environment.flota.id
    }
  }
}

resource "confluent_kafka_acl" "consumidor_read_topics" {
  kafka_cluster {
    id = confluent_kafka_cluster.flota.id
  }
  resource_type = "TOPIC"
  resource_name = "flota."
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.consumidor.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.flota.rest_endpoint
  credentials {
    key    = confluent_api_key.app_manager.id
    secret = confluent_api_key.app_manager.secret
  }
}

resource "confluent_kafka_acl" "consumidor_read_grupos" {
  kafka_cluster {
    id = confluent_kafka_cluster.flota.id
  }
  resource_type = "GROUP"
  resource_name = "rutas-"
  pattern_type  = "PREFIXED"
  principal     = "User:${confluent_service_account.consumidor.id}"
  host          = "*"
  operation     = "READ"
  permission    = "ALLOW"
  rest_endpoint = confluent_kafka_cluster.flota.rest_endpoint
  credentials {
    key    = confluent_api_key.app_manager.id
    secret = confluent_api_key.app_manager.secret
  }
}

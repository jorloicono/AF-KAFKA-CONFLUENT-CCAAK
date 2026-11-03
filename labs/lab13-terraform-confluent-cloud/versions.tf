terraform {
  required_version = ">= 1.5.0"
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

# Las credenciales de la Cloud API Key se leen de variables de entorno:
#   CONFLUENT_CLOUD_API_KEY / CONFLUENT_CLOUD_API_SECRET
provider "confluent" {}

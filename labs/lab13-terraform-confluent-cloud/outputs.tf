output "entorno_id" {
  value = confluent_environment.flota.id
}

output "cluster_id" {
  value = confluent_kafka_cluster.flota.id
}

output "bootstrap_endpoint" {
  value = confluent_kafka_cluster.flota.bootstrap_endpoint
}

output "productor_api_key" {
  value = confluent_api_key.productor.id
}

output "productor_api_secret" {
  value     = confluent_api_key.productor.secret
  sensitive = true
}

output "consumidor_api_key" {
  value = confluent_api_key.consumidor.id
}

output "consumidor_api_secret" {
  value     = confluent_api_key.consumidor.secret
  sensitive = true
}

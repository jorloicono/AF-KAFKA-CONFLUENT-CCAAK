variable "entorno" {
  description = "Nombre corto del entorno: dev, pre o pro"
  type        = string
  validation {
    condition     = contains(["dev", "pre", "pro"], var.entorno)
    error_message = "El entorno debe ser dev, pre o pro."
  }
}

variable "cloud" {
  description = "Proveedor de nube del clúster"
  type        = string
  default     = "AWS"
}

variable "region" {
  description = "Región del clúster"
  type        = string
  default     = "eu-west-1"
}

variable "particiones_posiciones" {
  description = "Particiones del topic de posiciones"
  type        = number
  default     = 6
}

variable "retencion_posiciones_ms" {
  description = "Retención del topic de posiciones en milisegundos"
  type        = number
  default     = 604800000 # 7 días
}

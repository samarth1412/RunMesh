variable "aws_region" {
  type    = string
  default = "us-east-1"
}
variable "environment" {
  type    = string
  default = "demo"
}
variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}
variable "db_instance_class" {
  type    = string
  default = "db.t4g.medium"
}
variable "redis_node_type" {
  type    = string
  default = "cache.t4g.small"
}
variable "kafka_brokers" {
  description = "Externally supplied Kafka bootstrap endpoints; this module deliberately does not provision MSK."
  type        = string
  validation {
    condition     = length(trimspace(var.kafka_brokers)) > 0
    error_message = "kafka_brokers must contain at least one external endpoint."
  }
}
variable "kubernetes_namespace" {
  type    = string
  default = "runmesh"
}
variable "kubernetes_service_account" {
  type    = string
  default = "runmesh"
}
variable "cloudwatch_log_retention_days" {
  type    = number
  default = 30
}
variable "domain_name" {
  description = "Optional public hostname for ACM and Route 53 integration."
  type        = string
  default     = ""
}
variable "route53_zone_id" {
  type    = string
  default = ""
}
variable "load_balancer_dns_name" {
  description = "Optional AWS Load Balancer Controller-created ALB DNS name."
  type        = string
  default     = ""
}
variable "load_balancer_zone_id" {
  type    = string
  default = ""
}

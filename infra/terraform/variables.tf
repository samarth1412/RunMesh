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

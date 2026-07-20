output "cluster_name" {
  value = module.eks.cluster_name
}
output "postgres_endpoint" {
  value = aws_db_instance.postgres.address
}
output "redis_endpoint" {
  value = aws_elasticache_replication_group.redis.primary_endpoint_address
}
output "artifact_bucket" {
  value = aws_s3_bucket.artifacts.id
}
output "ecr_repositories" {
  value = { for key, repository in aws_ecr_repository.images : key => repository.repository_url }
}
output "kafka_brokers" {
  description = "External Kafka endpoints forwarded to the RunMesh Helm release."
  value       = var.kafka_brokers
}
output "artifact_irsa_role_arn" {
  value = aws_iam_role.runmesh_artifacts.arn
}
output "load_balancer_controller_irsa_role_arn" {
  value = module.load_balancer_controller_irsa.iam_role_arn
}
output "application_log_group" {
  value = aws_cloudwatch_log_group.application.name
}
output "acm_certificate_arn" {
  value = try(aws_acm_certificate.runmesh[0].arn, null)
}

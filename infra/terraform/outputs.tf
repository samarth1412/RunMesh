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

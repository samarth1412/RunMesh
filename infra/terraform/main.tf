data "aws_availability_zones" "available" {
  state = "available"
}

module "vpc" {
  source               = "terraform-aws-modules/vpc/aws"
  version              = "5.17.0"
  name                 = "runmesh-${var.environment}"
  cidr                 = var.vpc_cidr
  azs                  = slice(data.aws_availability_zones.available.names, 0, 3)
  private_subnets      = ["10.42.1.0/24", "10.42.2.0/24", "10.42.3.0/24"]
  public_subnets       = ["10.42.101.0/24", "10.42.102.0/24", "10.42.103.0/24"]
  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true
  private_subnet_tags  = { "kubernetes.io/role/internal-elb" = "1" }
  public_subnet_tags   = { "kubernetes.io/role/elb" = "1" }
}

module "eks" {
  source                                 = "terraform-aws-modules/eks/aws"
  version                                = "20.31.6"
  cluster_name                           = "runmesh-${var.environment}"
  cluster_version                        = "1.31"
  cluster_endpoint_public_access         = true
  vpc_id                                 = module.vpc.vpc_id
  subnet_ids                             = module.vpc.private_subnets
  enable_irsa                            = true
  cluster_enabled_log_types              = ["api", "audit", "authenticator", "controllerManager", "scheduler"]
  cloudwatch_log_group_retention_in_days = var.cloudwatch_log_retention_days
  cluster_addons = {
    coredns                         = { most_recent = true }
    kube-proxy                      = { most_recent = true }
    vpc-cni                         = { most_recent = true }
    amazon-cloudwatch-observability = { most_recent = true }
  }
  eks_managed_node_groups = {
    system = {
      instance_types = ["m7i.large"]
      min_size       = 2
      max_size       = 6
      desired_size   = 2
      iam_role_additional_policies = {
        CloudWatchAgentServerPolicy = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
      }
    }
  }
}

module "load_balancer_controller_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "5.52.2"

  role_name                              = "runmesh-${var.environment}-aws-load-balancer-controller"
  attach_load_balancer_controller_policy = true
  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:aws-load-balancer-controller"]
    }
  }
}

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "main" {
  name       = "runmesh-${var.environment}"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_security_group" "data" {
  name   = "runmesh-data-${var.environment}"
  vpc_id = module.vpc.vpc_id
  ingress {
    from_port       = 5432
    to_port         = 5432
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }
  ingress {
    from_port       = 6379
    to_port         = 6379
    protocol        = "tcp"
    security_groups = [module.eks.node_security_group_id]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_instance" "postgres" {
  identifier             = "runmesh-${var.environment}"
  engine                 = "postgres"
  engine_version         = "17.2"
  instance_class         = var.db_instance_class
  allocated_storage      = 30
  storage_encrypted      = true
  db_name                = "runmesh"
  username               = "runmesh"
  password               = random_password.db.result
  db_subnet_group_name   = aws_db_subnet_group.main.name
  vpc_security_group_ids = [aws_security_group.data.id]
  multi_az               = false
  skip_final_snapshot    = true
  deletion_protection    = false
}

resource "aws_elasticache_subnet_group" "main" {
  name       = "runmesh-${var.environment}"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id       = "runmesh-${var.environment}"
  description                = "RunMesh cache and rate limits"
  node_type                  = var.redis_node_type
  port                       = 6379
  subnet_group_name          = aws_elasticache_subnet_group.main.name
  security_group_ids         = [aws_security_group.data.id]
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
  num_cache_clusters         = 2
  automatic_failover_enabled = true
}

resource "aws_s3_bucket" "artifacts" {
  bucket_prefix = "runmesh-${var.environment}-artifacts-"
  force_destroy = true
}
resource "aws_s3_bucket_public_access_block" "artifacts" {
  bucket                  = aws_s3_bucket.artifacts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_lifecycle_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload { days_after_initiation = 7 }
    noncurrent_version_expiration { noncurrent_days = 30 }
  }
}

data "aws_iam_policy_document" "artifact_assume_role" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [module.eks.oidc_provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "${replace(module.eks.cluster_oidc_issuer_url, "https://", "")}:sub"
      values   = ["system:serviceaccount:${var.kubernetes_namespace}:${var.kubernetes_service_account}"]
    }
  }
}

data "aws_iam_policy_document" "artifact_access" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.artifacts.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.artifacts.arn}/*"]
  }
}

resource "aws_iam_role" "runmesh_artifacts" {
  name               = "runmesh-${var.environment}-artifacts"
  assume_role_policy = data.aws_iam_policy_document.artifact_assume_role.json
}

resource "aws_iam_role_policy" "runmesh_artifacts" {
  name   = "artifact-bucket-access"
  role   = aws_iam_role.runmesh_artifacts.id
  policy = data.aws_iam_policy_document.artifact_access.json
}
resource "aws_s3_bucket_server_side_encryption_configuration" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_ecr_repository" "images" {
  for_each = toset(["control-plane", "scheduler", "worker-go", "worker-python", "web"])
  name     = "runmesh/${each.key}"
  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }
}

resource "aws_ecr_lifecycle_policy" "images" {
  for_each   = aws_ecr_repository.images
  repository = each.value.name
  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Expire untagged images"
        selection    = { tagStatus = "untagged", countType = "sinceImagePushed", countUnit = "days", countNumber = 14 }
        action       = { type = "expire" }
      },
      {
        rulePriority = 2
        description  = "Retain the newest 50 versioned images"
        selection    = { tagStatus = "tagged", tagPrefixList = ["v", "sha-"], countType = "imageCountMoreThan", countNumber = 50 }
        action       = { type = "expire" }
      }
    ]
  })
}

resource "aws_cloudwatch_log_group" "application" {
  name              = "/runmesh/${var.environment}/application"
  retention_in_days = var.cloudwatch_log_retention_days
}

resource "aws_cloudwatch_metric_alarm" "postgres_cpu" {
  alarm_name          = "runmesh-${var.environment}-postgres-high-cpu"
  namespace           = "AWS/RDS"
  metric_name         = "CPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 2
  comparison_operator = "GreaterThanThreshold"
  threshold           = 80
  dimensions          = { DBInstanceIdentifier = aws_db_instance.postgres.id }
}

resource "aws_cloudwatch_metric_alarm" "redis_cpu" {
  alarm_name          = "runmesh-${var.environment}-redis-high-cpu"
  namespace           = "AWS/ElastiCache"
  metric_name         = "EngineCPUUtilization"
  statistic           = "Average"
  period              = 300
  evaluation_periods  = 2
  comparison_operator = "GreaterThanThreshold"
  threshold           = 80
  dimensions          = { ReplicationGroupId = aws_elasticache_replication_group.redis.id }
}

resource "aws_acm_certificate" "runmesh" {
  count             = var.domain_name == "" ? 0 : 1
  domain_name       = var.domain_name
  validation_method = "DNS"
  lifecycle { create_before_destroy = true }
}

resource "aws_route53_record" "certificate_validation" {
  for_each = var.domain_name == "" || var.route53_zone_id == "" ? {} : {
    for option in aws_acm_certificate.runmesh[0].domain_validation_options : option.domain_name => {
      name   = option.resource_record_name
      record = option.resource_record_value
      type   = option.resource_record_type
    }
  }
  zone_id = var.route53_zone_id
  name    = each.value.name
  type    = each.value.type
  ttl     = 60
  records = [each.value.record]
}

resource "aws_acm_certificate_validation" "runmesh" {
  count                   = var.domain_name == "" || var.route53_zone_id == "" ? 0 : 1
  certificate_arn         = aws_acm_certificate.runmesh[0].arn
  validation_record_fqdns = [for record in aws_route53_record.certificate_validation : record.fqdn]
}

resource "aws_route53_record" "runmesh" {
  count   = var.domain_name == "" || var.route53_zone_id == "" || var.load_balancer_dns_name == "" || var.load_balancer_zone_id == "" ? 0 : 1
  zone_id = var.route53_zone_id
  name    = var.domain_name
  type    = "A"
  alias {
    name                   = var.load_balancer_dns_name
    zone_id                = var.load_balancer_zone_id
    evaluate_target_health = true
  }
}

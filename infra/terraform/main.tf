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
  source                         = "terraform-aws-modules/eks/aws"
  version                        = "20.31.6"
  cluster_name                   = "runmesh-${var.environment}"
  cluster_version                = "1.31"
  cluster_endpoint_public_access = true
  vpc_id                         = module.vpc.vpc_id
  subnet_ids                     = module.vpc.private_subnets
  enable_irsa                    = true
  eks_managed_node_groups = {
    system = { instance_types = ["m7i.large"], min_size = 2, max_size = 6, desired_size = 2 }
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
resource "aws_s3_bucket_versioning" "artifacts" {
  bucket = aws_s3_bucket.artifacts.id
  versioning_configuration { status = "Enabled" }
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
  for_each = toset(["control-plane", "scheduler", "worker-go", "web"])
  name     = "runmesh/${each.key}"
  image_scanning_configuration { scan_on_push = true }
  encryption_configuration { encryption_type = "AES256" }
}

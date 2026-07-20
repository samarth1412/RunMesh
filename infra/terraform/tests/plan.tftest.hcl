mock_provider "aws" {
  mock_data "aws_availability_zones" {
    defaults = {
      names = ["us-east-1a", "us-east-1b", "us-east-1c"]
    }
  }
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws:iam::123456789012:user/terraform-test"
      user_id    = "AIDATEST"
    }
  }
  mock_data "aws_partition" {
    defaults = {
      partition  = "aws"
      dns_suffix = "amazonaws.com"
    }
  }
  mock_data "aws_region" {
    defaults = { name = "us-east-1" }
  }
  mock_data "aws_iam_policy_document" {
    defaults = { json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}" }
  }
  mock_resource "aws_iam_role" {
    defaults = {
      arn  = "arn:aws:iam::123456789012:role/runmesh-test"
      id   = "runmesh-test"
      name = "runmesh-test"
    }
  }
  mock_resource "aws_iam_policy" {
    defaults = {
      arn = "arn:aws:iam::123456789012:policy/runmesh-test"
      id  = "arn:aws:iam::123456789012:policy/runmesh-test"
    }
  }
  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::runmesh-test-artifacts"
      id  = "runmesh-test-artifacts"
    }
  }
}

mock_provider "random" {}

run "production_plan_without_aws_credentials" {
  command = plan

  variables {
    environment   = "test"
    kafka_brokers = "kafka.example.internal:9092"
  }

  assert {
    condition     = length(aws_ecr_repository.images) == 5
    error_message = "all five release image repositories must be managed"
  }

  assert {
    condition     = aws_s3_bucket_public_access_block.artifacts.block_public_policy
    error_message = "artifact bucket public policies must be blocked"
  }

  assert {
    condition     = output.kafka_brokers == "kafka.example.internal:9092"
    error_message = "Kafka must remain an externally supplied endpoint"
  }

  assert {
    condition     = aws_cloudwatch_log_group.application.retention_in_days == 30
    error_message = "CloudWatch application retention must use the configured default"
  }
}

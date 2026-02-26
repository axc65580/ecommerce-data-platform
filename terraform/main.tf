terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# --- S3 Buckets (Delta Lake layers) ---
resource "aws_s3_bucket" "bronze" {
  bucket = "-bronze-"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "silver" {
  bucket = "-silver-"
  tags   = local.common_tags
}

resource "aws_s3_bucket" "gold" {
  bucket = "-gold-"
  tags   = local.common_tags
}

# --- S3 Versioning ---
resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration {
    status = "Enabled"
  }
}

# --- MSK (Managed Kafka) ---
resource "aws_msk_cluster" "ecommerce" {
  cluster_name           = "-kafka-"
  kafka_version          = "3.4.0"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.msk.id]

    storage_info {
      ebs_storage_info {
        volume_size = 20
      }
    }
  }

  tags = local.common_tags
}

# --- Security Group for MSK ---
resource "aws_security_group" "msk" {
  name        = "-msk-sg"
  description = "Security group for MSK Kafka cluster"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
  }

  tags = local.common_tags
}

# --- Glue Database (for Delta Lake catalog) ---
resource "aws_glue_catalog_database" "ecommerce" {
  name        = "_"
  description = "E-Commerce Data Platform catalog"
}

locals {
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

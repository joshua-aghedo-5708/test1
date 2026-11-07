# Proposed production deployment for NairaFlow (Terraform, AWS provider).
# DESIGN ARTEFACT ONLY: this file has NOT been applied to any cloud account.
# It documents the target architecture discussed in ../README.md section 5 and
# validates with `terraform validate` once `terraform init` has downloaded the provider.

terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.primary_region
}

provider "aws" {
  alias  = "dr"
  region = var.dr_region
}

variable "primary_region" { default = "af-south-1" }   # Cape Town: closest AWS region to Lagos
variable "dr_region"      { default = "eu-west-1" }    # disaster-recovery copy (subject to data-residency review)
variable "db_password"    { sensitive = true }

# --- System of record: PostgreSQL 17, Multi-AZ = synchronous standby in a second AZ ---
resource "aws_db_instance" "ledger" {
  identifier                   = "nairaflow-ledger"
  engine                       = "postgres"
  engine_version               = "17"
  instance_class               = "db.r6g.large"
  allocated_storage            = 200
  storage_type                 = "gp3"
  storage_encrypted            = true                  # encryption at rest (KMS)
  multi_az                     = true                  # sync replica + automatic failover (Portfolio 5 A4/A5)
  backup_retention_period      = 35                    # point-in-time recovery window, days
  deletion_protection          = true
  performance_insights_enabled = true
  username                     = "nairaflow_admin"
  password                     = var.db_password
  parameter_group_name         = aws_db_parameter_group.ledger.name
}

resource "aws_db_parameter_group" "ledger" {
  name   = "nairaflow-pg17"
  family = "postgres17"
  parameter {
    name  = "log_lock_waits"
    value = "1"
  }
  parameter {
    name  = "deadlock_timeout"
    value = "1000"
  }
}

# --- Read scaling: async read replica for statements/reporting (Portfolio 5 A3) ---
resource "aws_db_instance" "ledger_read" {
  identifier          = "nairaflow-ledger-read-1"
  replicate_source_db = aws_db_instance.ledger.identifier
  instance_class      = "db.r6g.large"
  storage_encrypted   = true
}

# --- Cache, idempotency keys, velocity limits, sessions (Portfolio 4 Redis) ---
resource "aws_elasticache_replication_group" "cache" {
  replication_group_id       = "nairaflow-cache"
  description                = "NairaFlow Redis: cache, idempotency, rate limits"
  engine                     = "redis"
  node_type                  = "cache.r7g.large"
  num_cache_clusters         = 2                       # primary + replica in another AZ
  automatic_failover_enabled = true
  multi_az_enabled           = true
  at_rest_encryption_enabled = true
  transit_encryption_enabled = true
}

# MongoDB (customer profiles / statements, Portfolio 4) would run on MongoDB Atlas as a
# 3-node replica set spread over 3 AZs (Portfolio 5 C), configured with the
# mongodbatlas provider; omitted here to keep a single provider.

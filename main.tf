terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.2.0"
    }
  }
  required_version = ">= 1.3"
}

provider "aws" {
  region = "us-east-1"
}


#Crear recurso s3
resource "aws_s3_bucket" "data_bucket" {
  bucket = "netflix-datasets-bs3"
  tags = {
    Name        = "netflix-bucket-prod"
    Environment = "prod"
  }
}


# Versión de objetos (para historial de CSVs)
resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

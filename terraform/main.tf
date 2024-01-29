terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  region = "eu-west-3"
}


resource "aws_s3_bucket" "video-games-etl-bucket" {
  bucket = var.bucket_name
}

resource "aws_s3_object" "raw_data" {
  bucket = aws_s3_bucket.video-games-etl-bucket.id
  key    = var.data_key
  source = var.local_path_data
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.video-games-etl-bucket.id
  key    = var.script_key
  source = var.local_path_script
}


resource "aws_glue_catalog_database" "video-games-database" {
  name = var.database_name
}


resource "aws_iam_role" "glue_iam_role" {
  name = "glue_iam_role_vg_sales"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      },
    ]
  })

  managed_policy_arns = ["arn:aws:iam::aws:policy/AmazonS3FullAccess", "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"]
}


resource "aws_glue_crawler" "vg-crawler" {
  database_name = aws_glue_catalog_database.video-games-database.name
  name          = "video-game-data"
  role          = aws_iam_role.glue_iam_role.arn
  s3_target {
    path = "s3://${aws_s3_bucket.video-games-etl-bucket.bucket}/${var.path_to_data_key}"
  }
}


resource "aws_glue_job" "vg-etl-job" {
  name     = "vg-etl-job"
  role_arn = aws_iam_role.glue_iam_role.arn
  glue_version = "4.0"
  worker_type  = "G.1X"
  number_of_workers = 2
  timeout = 2880

  command {
    script_location = "s3://${aws_s3_bucket.video-games-etl-bucket.bucket}/${var.script_key}"
  }
}
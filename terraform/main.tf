terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

provider "google" {
  project = var.PROJECT
  region = var.REGION
  // credentials = file(var.credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

resource "google_storage_bucket" "data_lake_bucket" {
  name          = var.BUCKET
  location      = var.REGION

  # Optional, but recommended settings:
  storage_class = var.STORAGE_CLASS
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90  // days
    }
  }

  force_destroy = true
}

resource "google_storage_bucket_object" "shot_charts_folder" {
  name          = var.BUCKET_SHOT_CHARTS_FOLDER
  content       = "folder_content"
  bucket        = google_storage_bucket.data_lake_bucket.name
}

resource "google_storage_bucket_object" "player_info_folder" {
  name          = var.BUCKET_PLAYER_INFO_FOLDER
  content       = "folder_content"
  bucket        = google_storage_bucket.data_lake_bucket.name
}

resource "google_storage_bucket_object" "teams_folder" {
  name          = var.BUCKET_TEAMS_FOLDER
  content       = "folder_content"
  bucket        = google_storage_bucket.data_lake_bucket.name
}

resource "google_bigquery_dataset" "dataset_raw" {
  dataset_id = var.BQ_DATASET_RAW
  project    = var.PROJECT
  location   = var.REGION
}

resource "google_bigquery_dataset" "dataset_stg" {
  dataset_id = var.BQ_DATASET_STG
  project    = var.PROJECT
  location   = var.REGION
}

resource "google_bigquery_dataset" "dataset_prod" {
  dataset_id = var.BQ_DATASET_PROD
  project    = var.PROJECT
  location   = var.REGION
}
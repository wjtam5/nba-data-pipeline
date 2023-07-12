variable "PROJECT" {
  description = "NBA Data Pipeline Project ID"
}

variable "REGION" {
  description = "GCP resource region"
  type = string
}

variable "BUCKET" {
  description = "The data lake in GCS where raw files are stored"
  type = string
}

variable "STORAGE_CLASS" {
  description = "Storage class type for your bucket"
  default = "STANDARD"
}

variable "BUCKET_SHOT_CHARTS_FOLDER" {
  description = "Name of the folder in GCS bucket to store shot chart files"
  type = string
}

variable "BUCKET_PLAYER_INFO_FOLDER" {
  description = "Name of the folder in GCS bucket to store player info files"
  type = string
}

variable "BUCKET_TEAMS_FOLDER" {
  description = "Name of the folder in GCS bucket to store team files"
  type = string
}

variable "BQ_DATASET_RAW" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
}

variable "BQ_DATASET_STG" {
  description = "BigQuery Dataset that staging data (from raw) will be written to"
  type = string
}

variable "BQ_DATASET_PROD" {
  description = "BigQuery Dataset that prod data (from staging) will be written to"
  type = string
}


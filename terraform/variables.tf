variable "local_path_data" {
  type        = string
  default     = "../data/vgsales.csv"
}

variable "local_path_script" {
  type        = string
  default     = "../glue_etl_job/glue_etl_job_spark_script.py"
}

variable "bucket_name" {
  type        = string
  default     = "video-game-etl"
}


variable "data_key" {
  type        = string
  default     = "raw_data/vg_sales.csv"
}

variable "script_key" {
  type        = string
  default     = "script/glue_etl_script.py"
}

variable "path_to_data_key" {
  type        = string
  default     = "raw_data"
}


variable "database_name" {
  type        = string
  default     = "video-game-sales-database"
}



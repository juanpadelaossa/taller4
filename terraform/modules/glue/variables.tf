variable "project" {}
variable "env" {}
variable "glue_role_arn" {}

variable "bronze_bucket" {}
variable "silver_bucket" {}
variable "temp_bucket" {}

variable "tags" {
  type    = map(string)
  default = {}
}
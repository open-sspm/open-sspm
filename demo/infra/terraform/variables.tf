variable "project_id" {
  description = "Scaleway Project ID. If empty, provider may fall back to SCW_DEFAULT_PROJECT_ID."
  type        = string
  default     = ""
}

variable "zone" {
  description = "Scaleway zone."
  type        = string
  default     = "fr-par-2"
}

variable "server_name" {
  description = "Instance name."
  type        = string
  default     = "open-sspm-demo"
}

variable "instance_type" {
  description = "Scaleway instance type."
  type        = string
  default     = "DEV1-S"
}

variable "image" {
  description = "Scaleway instance image identifier."
  type        = string
  default     = "ubuntu_noble"
}

variable "ssh_public_key" {
  description = "SSH public key to authorize for the deploy user (single line)."
  type        = string
}

variable "admin_cidr" {
  description = "CIDR allowed to SSH to the server."
  type        = string
  default     = "0.0.0.0/0"
}


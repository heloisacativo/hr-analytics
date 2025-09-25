variable "compartment_id" {
  description = "OCID do Compartment onde os recursos serão criados"
  type        = string
}

variable "tenancy_ocid" {
  description = "OCID da tenancy (raiz da sua conta OCI)"
  type        = string
}

variable "db_display_name" {
  description = "Nome para exibir do Autonomous Database"
  type        = string
}

variable "db_name" {
  description = "Nome do banco (até 14 caracteres, letras maiúsculas, sem espaços)"
  type        = string
}

variable "cpu_core_count" {
  description = "Número de CPUs"
  type        = number
  default     = 1
}

variable "data_storage_size_in_tbs" {
  description = "Tamanho do armazenamento (em TB)"
  type        = number
  default     = 1
}

variable "subnet_id" {
  description = "OCID da subnet da VCN onde o ADB será criado (para endpoint privado)"
  type        = string
}

variable "adb_admin_password" {
  type        = string
  sensitive   = true
  description = "Senha do usuário ADMIN (12–30 chars, 1 maiúscula, 1 minúscula, 1 número; não conter \"admin\" nem \")"
}

variable "adb_wallet_password" {
  type        = string
  sensitive   = true
  description = "Senha para criptografar o wallet ZIP (mín. 8; 1 letra + (1 número ou 1 especial))"
}

variable "whitelisted_ips" {
  type        = list(string)
  default     = []     
}



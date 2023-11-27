terraform {
  required_version = ">=1.0"

  required_providers {
    azapi = {
      source  = "azure/azapi"
      version = "~>1.5"
    }
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~>3.0"
    }
  }
}

provider "azurerm" {
  features {}

  subscription_id = "c4d4e949-68be-4c6c-9342-3454262b7887"
  tenant_id       = "ca71eddc-cc7b-4e5b-95bd-55b658e696be"
  client_id       = "87de67cf-a1ef-4228-beef-12b07e79d058"
  client_secret   = "E4X8Q~PinFgBvueCLstzB8L6jHpf_mSJjVeKkaEr"
}

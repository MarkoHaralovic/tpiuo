resource "azurerm_resource_group" "resource_group" {
  name = "fervjestina"
  location = "West Europe"
}

resource "azurerm_kubernetes_cluster" "aks_cluster" {

  name                = "tpiuoAKS"
  location            = azurerm_resource_group.resource_group.location
  resource_group_name = azurerm_resource_group.resource_group.name
  dns_prefix          = "tpiuoAKS-fervjestina-c4d4e9"

  default_node_pool {
    name       = "default" 
    node_count = 1       
    vm_size    = "Standard_DS2_v2"  
  }

  identity {
    type = "SystemAssigned"
  }

  linux_profile {
    admin_username = "azureadmin"

    ssh_key {
      key_data = jsondecode(azapi_resource_action.ssh_public_key_gen.output).publicKey
    }
  }
}

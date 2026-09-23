param location string = resourceGroup().location
param storageAccountName string
param cosmosAccountName string
param eventHubNamespaceName string
param eventHubName string = 'vsam-changes'
param databaseName string = 'mainframeOffload'
param balancesContainerName string = 'balances'
param quarantineContainerName string = 'sync-quarantine'
param consumerGroupName string = 'guided-demo'
param appName string = 'app-vsam-demo'
param deployApplication bool = false
param imageTag string = 'demo'

@secure()
param demoAccessToken string

var tags = {
  project: 'vsam-offload-demo'
  environment: 'non-production'
}
var blobDataContributor = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
)
var hubDataSender = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  '2b629674-e913-4c01-ae53-ef4638d8f975'
)
var hubDataReceiver = subscriptionResourceId(
  'Microsoft.Authorization/roleDefinitions',
  'a638d3c7-ab3a-418d-83e6-5f17a39d4fde'
)
var acrPull = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}
resource blobs 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' existing = {
  parent: storage
  name: 'default'
}
resource raw 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' existing = {
  parent: blobs
  name: 'vsam-raw'
}
resource control 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobs
  name: 'demo-control'
  properties: {
    publicAccess: 'None'
  }
}
resource cosmos 'Microsoft.DocumentDB/databaseAccounts@2024-05-15' existing = {
  name: cosmosAccountName
}
resource database 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2024-05-15' existing = {
  parent: cosmos
  name: databaseName
}
resource quarantine 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = {
  parent: database
  name: quarantineContainerName
  properties: {
    resource: {
      id: quarantineContainerName
      partitionKey: {
        paths: [
          '/accountId'
        ]
        kind: 'Hash'
      }
    }
    options: {
      throughput: 400
    }
  }
}
resource hubNamespace 'Microsoft.EventHub/namespaces@2024-01-01' existing = {
  name: eventHubNamespaceName
}
resource hub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' existing = {
  parent: hubNamespace
  name: eventHubName
}
resource consumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  parent: hub
  name: consumerGroupName
}

resource nsg 'Microsoft.Network/networkSecurityGroups@2024-05-01' = {
  name: 'nsg-vsam-demo'
  location: location
  tags: tags
}
resource vnet 'Microsoft.Network/virtualNetworks@2024-05-01' = {
  name: 'vnet-vsam-demo'
  location: location
  tags: tags
  properties: {
    addressSpace: {
      addressPrefixes: [
        '10.93.0.0/16'
      ]
    }
    subnets: [
      {
        name: 'app-integration'
        properties: {
          addressPrefix: '10.93.1.0/24'
          networkSecurityGroup: {
            id: nsg.id
          }
          delegations: [
            {
              name: 'container-apps'
              properties: {
                serviceName: 'Microsoft.App/environments'
              }
            }
          ]
        }
      }
      {
        name: 'private-endpoints'
        properties: {
          addressPrefix: '10.93.2.0/24'
          privateEndpointNetworkPolicies: 'Disabled'
          networkSecurityGroup: {
            id: nsg.id
          }
        }
      }
    ]
  }
}

resource blobDns 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: 'privatelink.blob.${environment().suffixes.storage}'
  location: 'global'
  tags: tags
}
resource cosmosDns 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: 'privatelink.documents.azure.com'
  location: 'global'
  tags: tags
}
resource hubDns 'Microsoft.Network/privateDnsZones@2020-06-01' = {
  name: 'privatelink.servicebus.windows.net'
  location: 'global'
  tags: tags
}
resource hubLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: hubDns
  name: 'vsam-demo'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}
resource blobLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: blobDns
  name: 'vsam-demo'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}
resource cosmosLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = {
  parent: cosmosDns
  name: 'vsam-demo'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: vnet.id
    }
  }
}
resource blobEndpoint 'Microsoft.Network/privateEndpoints@2024-05-01' = {
  name: 'pe-vsam-blob'
  location: location
  tags: tags
  properties: {
    subnet: {
      id: '${vnet.id}/subnets/private-endpoints'
    }
    privateLinkServiceConnections: [
      {
        name: 'blob'
        properties: {
          privateLinkServiceId: storage.id
          groupIds: [
            'blob'
          ]
        }
      }
    ]
  }
}
resource cosmosEndpoint 'Microsoft.Network/privateEndpoints@2024-05-01' = {
  name: 'pe-vsam-cosmos'
  location: location
  tags: tags
  properties: {
    subnet: {
      id: '${vnet.id}/subnets/private-endpoints'
    }
    privateLinkServiceConnections: [
      {
        name: 'cosmos'
        properties: {
          privateLinkServiceId: cosmos.id
          groupIds: [
            'Sql'
          ]
        }
      }
    ]
  }
}
resource hubEndpoint 'Microsoft.Network/privateEndpoints@2024-05-01' = {
  name: 'pe-vsam-eventhubs'
  location: location
  tags: tags
  properties: {
    subnet: {
      id: '${vnet.id}/subnets/private-endpoints'
    }
    privateLinkServiceConnections: [
      {
        name: 'eventhubs'
        properties: {
          privateLinkServiceId: hubNamespace.id
          groupIds: [
            'namespace'
          ]
        }
      }
    ]
  }
}
resource blobZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-05-01' = {
  parent: blobEndpoint
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'blob'
        properties: {
          privateDnsZoneId: blobDns.id
        }
      }
    ]
  }
}
resource cosmosZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-05-01' = {
  parent: cosmosEndpoint
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'cosmos'
        properties: {
          privateDnsZoneId: cosmosDns.id
        }
      }
    ]
  }
}
resource hubZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-05-01' = {
  parent: hubEndpoint
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'eventhubs'
        properties: {
          privateDnsZoneId: hubDns.id
        }
      }
    ]
  }
}

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  name: 'id-vsam-demo'
  location: location
  tags: tags
}
resource registry 'Microsoft.ContainerRegistry/registries@2023-07-01' = {
  name: 'acrvsam${uniqueString(resourceGroup().id)}'
  location: location
  tags: tags
  sku: {
    name: 'Standard'
  }
  properties: {
    adminUserEnabled: false
  }
}
resource registryPullRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: registry
  name: guid(registry.id, identity.id, acrPull)
  properties: {
    roleDefinitionId: acrPull
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}
resource logs 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: 'log-vsam-demo'
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}
resource appEnvironment 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: 'cae-vsam-demo'
  location: location
  tags: tags
  properties: {
    vnetConfiguration: {
      infrastructureSubnetId: '${vnet.id}/subnets/app-integration'
      internal: false
    }
    workloadProfiles: [
      {
        name: 'Consumption'
        workloadProfileType: 'Consumption'
      }
    ]
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logs.properties.customerId
        sharedKey: logs.listKeys().primarySharedKey
      }
    }
  }
}
resource app 'Microsoft.App/containerApps@2024-03-01' = if (deployApplication) {
  name: appName
  location: location
  tags: tags
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
  properties: {
    managedEnvironmentId: appEnvironment.id
    workloadProfileName: 'Consumption'
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: {
        external: true
        targetPort: 8000
        transport: 'auto'
        allowInsecure: false
      }
      secrets: [
        { name: 'demo-access-token', value: demoAccessToken }
      ]
      registries: [
        {
          server: registry.properties.loginServer
          identity: identity.id
        }
      ]
    }
    template: {
      containers: [
        {
          name: 'guided-demo'
          image: '${registry.properties.loginServer}/vsam-guided:${imageTag}'
          resources: {
            cpu: json('0.5')
            memory: '1Gi'
          }
          env: [
            { name: 'DEMO_ACCESS_TOKEN', secretRef: 'demo-access-token' }
            { name: 'AZURE_CLIENT_ID', value: identity.properties.clientId }
            { name: 'STORAGE_ACCOUNT_URL', value: storage.properties.primaryEndpoints.blob }
            { name: 'RAW_CONTAINER', value: raw.name }
            { name: 'CONTROL_CONTAINER', value: control.name }
            { name: 'COSMOS_ENDPOINT', value: cosmos.properties.documentEndpoint }
            { name: 'COSMOS_DATABASE', value: databaseName }
            { name: 'COSMOS_CONTAINER', value: balancesContainerName }
            { name: 'COSMOS_QUARANTINE_CONTAINER', value: quarantine.name }
            { name: 'EVENTHUB_FULLY_QUALIFIED_NAMESPACE', value: '${hubNamespace.name}.servicebus.windows.net' }
            { name: 'EVENTHUB_NAME', value: hub.name }
            { name: 'EVENTHUB_CONSUMER_GROUP', value: consumerGroup.name }
            { name: 'AZURE_SUBSCRIPTION_ID', value: subscription().subscriptionId }
            { name: 'AZURE_RESOURCE_GROUP', value: resourceGroup().name }
            { name: 'AZURE_LOCATION', value: location }
            { name: 'AZURE_WEBAPP_NAME', value: appName }
            { name: 'AZURE_CONTAINER_APP_NAME', value: appName }
            { name: 'AZURE_COMPUTE_KIND', value: 'containerapp' }
          ]
          probes: [
            {
              type: 'Liveness'
              httpGet: {
                path: '/healthz'
                port: 8000
              }
              initialDelaySeconds: 20
              periodSeconds: 30
            }
            {
              type: 'Readiness'
              httpGet: {
                path: '/healthz'
                port: 8000
              }
              initialDelaySeconds: 5
              periodSeconds: 10
            }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
  dependsOn: [
    registryPullRole
  ]
}

resource rawRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: raw
  name: guid(raw.id, identity.id, blobDataContributor)
  properties: {
    roleDefinitionId: blobDataContributor
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}
resource controlRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: control
  name: guid(control.id, identity.id, blobDataContributor)
  properties: {
    roleDefinitionId: blobDataContributor
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}
resource senderRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: hub
  name: guid(hub.id, identity.id, hubDataSender)
  properties: {
    roleDefinitionId: hubDataSender
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}
resource receiverRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: hub
  name: guid(hub.id, identity.id, hubDataReceiver)
  properties: {
    roleDefinitionId: hubDataReceiver
    principalId: identity.properties.principalId
    principalType: 'ServicePrincipal'
  }
}
resource cosmosRole 'Microsoft.DocumentDB/databaseAccounts/sqlRoleAssignments@2024-05-15' = {
  parent: cosmos
  name: guid(cosmos.id, identity.id, 'guided-demo-data')
  properties: {
    roleDefinitionId: '${cosmos.id}/sqlRoleDefinitions/00000000-0000-0000-0000-000000000002'
    principalId: identity.properties.principalId
    scope: '${cosmos.id}/dbs/${databaseName}'
  }
  dependsOn: [
    quarantine
  ]
}

output appName string = appName
output webUrl string = deployApplication ? 'https://${app!.properties.configuration.ingress.fqdn}' : ''
output registryName string = registry.name
output registryServer string = registry.properties.loginServer
output environmentName string = appEnvironment.name
output principalId string = identity.properties.principalId
output privateNetworkName string = vnet.name
output rawContainerName string = raw.name
output controlContainerName string = control.name
output consumerGroupName string = consumerGroup.name

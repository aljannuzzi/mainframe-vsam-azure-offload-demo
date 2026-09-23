param environmentName string = 'mfvsamdemo'
param location string = resourceGroup().location
param cosmosDatabaseName string = 'mainframeOffload'
param cosmosContainerName string = 'balances'
param consumerGroupName string = 'sync-demo'
param quarantineContainerName string = 'sync-quarantine'

var suffix = uniqueString(resourceGroup().id, environmentName)
var storageName = toLower('st${environmentName}${suffix}')
var eventHubNamespaceName = toLower('ehns-${environmentName}-${suffix}')
var cosmosAccountName = toLower('cosmos-${environmentName}-${suffix}')

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: take(storageName, 24)
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    publicNetworkAccess: 'Disabled'
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  name: 'default'
  parent: storage
}

resource rawContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: 'vsam-raw'
  parent: blobService
  properties: {
    publicAccess: 'None'
  }
}

resource checkpoints 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: 'sync-checkpoints'
  parent: blobService
  properties: {
    publicAccess: 'None'
  }
}

resource eventHubNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: eventHubNamespaceName
  location: location
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
  properties: {
    minimumTlsVersion: '1.2'
    disableLocalAuth: true
  }
}

resource consumerGroup 'Microsoft.EventHub/namespaces/eventhubs/consumergroups@2024-01-01' = {
  name: consumerGroupName
  parent: eventHub
}

resource eventHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  name: 'vsam-changes'
  parent: eventHubNamespace
  properties: {
    partitionCount: 2
    messageRetentionInDays: 1
  }
}

resource cosmos 'Microsoft.DocumentDB/databaseAccounts@2024-05-15' = {
  name: cosmosAccountName
  location: location
  kind: 'GlobalDocumentDB'
  properties: {
    databaseAccountOfferType: 'Standard'
    disableLocalAuth: true
    publicNetworkAccess: 'Disabled'
    locations: [
      {
        locationName: location
        failoverPriority: 0
        isZoneRedundant: false
      }
    ]
    consistencyPolicy: {
      defaultConsistencyLevel: 'Session'
    }
  }
}

resource database 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases@2024-05-15' = {
  name: cosmosDatabaseName
  parent: cosmos
  properties: {
    resource: {
      id: cosmosDatabaseName
    }
  }
}

resource container 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = {
  name: cosmosContainerName
  parent: database
  properties: {
    resource: {
      id: cosmosContainerName
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

resource quarantine 'Microsoft.DocumentDB/databaseAccounts/sqlDatabases/containers@2024-05-15' = {
  name: quarantineContainerName
  parent: database
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

output storageAccountName string = storage.name
output rawContainerName string = rawContainer.name
output eventHubNamespace string = eventHubNamespace.name
output eventHubName string = eventHub.name
output cosmosEndpoint string = cosmos.properties.documentEndpoint
output cosmosDatabase string = database.name
output cosmosContainer string = container.name
output consumerGroupName string = consumerGroup.name
output checkpointBlobUrl string = storage.properties.primaryEndpoints.blob
output checkpointContainerName string = checkpoints.name
output quarantineContainerName string = quarantine.name

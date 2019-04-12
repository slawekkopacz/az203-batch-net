$rgname = "rg-batch"
$location = "westeurope"
$saname = "batchstoraccount203"
$baname = "batchaccount203"

az login

az account set -s "Visual Studio Professional"

## Create rg
az group create -n $rgname -l $location

## Create storage account
az storage account create `
-n $saname `
-g $rgname `
--sku "Standard_LRS"

## Get storage account key
$sakey = az storage account keys list `
-g $rgname --account-name $saname `
--query "[0].value" -o tsv

$sakey

## Create blob containers

az storage container create `
-n inputfiles `
--account-key $sakey `
--account-name $saname `
--public-access blob

az storage container create `
-n outputfiles `
--account-key $sakey `
--account-name $saname

az storage container create `
-n failedfiles `
--account-key $sakey `
--account-name $saname


## Create Batch account (without storage account provided for auto starge)
az batch account create `
-n $baname `
-g $rgname `
-l $location


## Get Batch url
$batchurl = az batch account show `
-n $baname -g $rgname `
--query "accountEndpoint" `
-o tsv

$batchurl

## Get Batch key
$batchkey = az batch account keys list `
-n $baname -g $rgname `
--query "primary" `
-o tsv

$batchkey


az batch task show `
--job-id jobId1234 `
--task-id Task0 `
--account-endpoint "https://batchaccount203.westeurope.batch.azure.com" `
--account-key $batchkey `
--account-name $baname
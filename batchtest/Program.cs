using System;

namespace batchtest
{
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Batch;
    using Microsoft.Azure.Batch.Auth;
    using Microsoft.Azure.Batch.Common;
    using Microsoft.Extensions.Configuration;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Auth;
    using Microsoft.WindowsAzure.Storage.Blob;

    class Program
    {
        static async Task Main(string[] args)
        {

            var builder = new ConfigurationBuilder()
               .SetBasePath(Directory.GetCurrentDirectory())
               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
               .AddUserSecrets<Program>()
               .AddEnvironmentVariables();

            var configuration = builder.Build();

            // App settings. If you can't find the key in appsettings.json it means it was set using dotnet user-secrets
            // See: https://docs.microsoft.com/en-us/aspnet/core/security/app-secrets?view=aspnetcore-2.2&tabs=windows

            var batchAccountName = configuration["BatchAccount:Name"];
            var batchAccountUrl = configuration["BatchAccount:Url"];
            var batchAccountKey = configuration["BatchAccount:Key"];
            var storageAccountName = configuration["StorageAccount:Name"];
            var storageAccountUrl = configuration["StorageAccount:Url"];
            var storageAccountKey = configuration["StorageAccount:Key"];

            // Something like: @"https://batchstoraccount203.blob.core.windows.net/outputfiles?sv=....";
            var outputFilesContainerSasUrl = configuration["OutputFilesContainerSasUrl"];
            var failedFilesContainerSasUrl = configuration["FailedFilesContainerSasUrl"];

            var poolId = "poolId1234";
            var jobId = "jobId1234";

            // Upload input files on storage
            await UploadFiles(storageAccountName, storageAccountKey);

            BatchSharedKeyCredentials batchCred = new BatchSharedKeyCredentials(batchAccountUrl, batchAccountName, batchAccountKey);
            using (BatchClient batchClient = BatchClient.Open(batchCred))
            {

                // batchClient.CustomBehaviors.Add(RetryPolicyProvider.NoRetryProvider());

                await CreatePool(batchClient, poolId);
                await CreateJob(batchClient, jobId, poolId);

                var tasks = CreateTasks(storageAccountUrl, jobId, outputFilesContainerSasUrl, failedFilesContainerSasUrl);

                try
                {
                    // Run tasks (by adding them to the job)
                    await batchClient.JobOperations.AddTaskAsync(jobId, tasks,
                        parallelOptions: new BatchClientParallelOptions{MaxDegreeOfParallelism = 3, CancellationToken = CancellationToken.None});
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }

            Console.ReadLine();
        }

        private static async Task UploadFiles(string storageAccountName, string storageAccountKey)
        {
            StorageCredentials storageCredentials = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount storageAccount = new CloudStorageAccount(storageCredentials, useHttps: true);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference("inputfiles");
            await container.CreateIfNotExistsAsync();

            List<string> inputFileNames = new List<string>
            {
                "taskdata0.txt",
                "taskdata1.txt",
                "taskdata2.txt"
            };

            foreach (var fileName in inputFileNames)
            {
                using (FileStream fileStream = new FileStream(Path.Combine("inputFiles", fileName), FileMode.Open))
                {
                    CloudBlockBlob blob = container.GetBlockBlobReference(fileName);
                    await blob.UploadFromStreamAsync(fileStream);
                }
            }
        }

        private static async Task CreatePool(BatchClient batchClient, string poolId)
        {
            var imageReference = new ImageReference(
                    publisher: "Canonical",
                    offer: "UbuntuServer",
                    sku: "16.04-LTS",
                    version: "latest");

            var vmConfiguration = new VirtualMachineConfiguration(
                imageReference: imageReference,
                nodeAgentSkuId: "batch.node.ubuntu 16.04");

            try
            {
                CloudPool pool = batchClient.PoolOperations.CreatePool(
                    poolId: poolId,
                    targetDedicatedComputeNodes: 1,
                    virtualMachineSize: "Standard_A1_v2",
                    virtualMachineConfiguration: vmConfiguration);

                await pool.CommitAsync();
            }
            catch(BatchException ex)
            {
                if (ex.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.PoolExists)
                {
                    Console.WriteLine("The pool already existed when we tried to create it");
                }
            }
        }

        private static async Task CreateJob(BatchClient batchClient, string jobId, string poolId)
        {
            try
            {
                CloudJob job = batchClient.JobOperations.CreateJob();
                    job.Id = jobId;
                    job.PoolInformation = new PoolInformation { PoolId = poolId };

                    await job.CommitAsync();
            }
            catch(BatchException ex)
            {
                if (ex.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.JobExists)
                {
                    Console.WriteLine("The job already existed when we tried to create it");
                }
            }
        }

        private static IList<CloudTask> CreateTasks(string storageAccountUrl, string jobId, string outputFilesContainerSasUrl, string failedFilesContainerSasUrl)
        {
            var tasks = new List<CloudTask>();

            for (var i = 0; i < 3; i++)
            {
                var taskId = $"Task{i}";
                var inputFilePath = $"/inputfiles/taskdata{i}.txt";
                var inputFileBlobContainerUrl = storageAccountUrl + inputFilePath;
                var targetInputFileName = "input.txt";
                var taskCommandLine = "cp input.txt output.txt";

                CloudTask task = new CloudTask(taskId, taskCommandLine);

                // Input file (pointing to specific file within container and target name in working directory)
                task.ResourceFiles = new List<ResourceFile> {
                    ResourceFile.FromUrl(inputFileBlobContainerUrl, targetInputFileName)
                };

                // Output file destinations depending on result
                task.OutputFiles = new List<OutputFile> {
                    new OutputFile(
                        filePattern: "output.txt",
                        destination: new OutputFileDestination (
                            container: new OutputFileBlobContainerDestination(
                                outputFilesContainerSasUrl,
                                $"output-{jobId}-{taskId}.txt")
                        ),
                        uploadOptions: new OutputFileUploadOptions(uploadCondition: OutputFileUploadCondition.TaskSuccess)
                    ),
                    new OutputFile(
                        filePattern: "../std*.txt",
                        destination: new OutputFileDestination (
                            container: new OutputFileBlobContainerDestination(
                                failedFilesContainerSasUrl,
                                $"failed-{jobId}-{taskId}.txt")
                        ),
                        uploadOptions: new OutputFileUploadOptions(uploadCondition: OutputFileUploadCondition.TaskFailure)
                    )
                };

                tasks.Add(task);
            }

            return tasks;
        }
    }
}

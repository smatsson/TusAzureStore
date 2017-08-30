using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using tusdotnet.Interfaces;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using TusAzureStore.Constants;
using TusAzureStore.Helpers;
using TusAzureStore.Models;

namespace TusAzureStore.Stores
{
    // TODO: Class must be disposable to reset chunkFilesCompleted
    public class TusAzureBlobStore : ITusStore, ITusCreationStore, ITusReadableStore
    {
        private readonly Lazy<CloudBlobClient> _client;
        private readonly string _containerName;
        private readonly string _tempFileFolderPath;
        private int _chunkFilesCompleted;

        /*
         * The size of an append blob block size (4 MB).
         * This is the optimal batch size to send to Azure as this will minimize the number 
         * of blocks being created for a single file resulting in faster reads later on.
         */
        private const int AppendBlobBlockSize = 4194304;

        public TusAzureBlobStore(string connectionString, string containerName, string tempFileFolderPath)
        {
            _client = new Lazy<CloudBlobClient>(() => AzureStorageClientFactory.GetBlobClient(connectionString));
            _containerName = containerName;
            _tempFileFolderPath = tempFileFolderPath;
        }

        public async Task<long> AppendDataAsync(string fileId, Stream stream, CancellationToken cancellationToken)
        {
            var file = await GetAppendBlobReferenceWithAttributes(fileId, cancellationToken);

            var uploadLength = long.Parse(file.Metadata[AzureMetadata.UploadLength]);
            var fileLength = file.Properties.Length == -1 ? 0 : file.Properties.Length;

            if (uploadLength == fileLength)
            {
                return 0;
            }

            var bytesWritten = 0L;

            var tempPath = Path.Combine(_tempFileFolderPath, fileId);
            if (!Directory.Exists(tempPath))
            {
                Directory.CreateDirectory(tempPath);
            }

            var amapBuffer = new AmapStreamReader(AppendBlobBlockSize, stream);

            var writeToAzureCancellationSource = new CancellationTokenSource();

            var writeTask = StartWriteToAzure(file, tempPath, writeToAzureCancellationSource.Token);

            try
            {
                do
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        writeToAzureCancellationSource.Cancel();
                        break;
                    }

                    await amapBuffer.Read(cancellationToken);

                    if (amapBuffer.BytesRead == 0)
                    {
                        writeToAzureCancellationSource.Cancel();
                        break;
                    }

                    File.WriteAllBytes(Path.Combine(tempPath, (_chunkFilesCompleted + 1).ToString()), amapBuffer.Data);
                    Interlocked.Increment(ref _chunkFilesCompleted);

                    bytesWritten += amapBuffer.BytesRead;

                    if (amapBuffer.LoadAbortedException != null)
                    {
                        throw amapBuffer.LoadAbortedException;
                    }

                    if (amapBuffer.LoadAborted)
                    {
                        writeToAzureCancellationSource.Cancel();
                        break;
                    }

                    if (writeTask.IsFaulted || writeTask.IsCanceled)
                    {
                        if (writeTask.Exception != null)
                        {
                            throw writeTask.Exception;
                        }
                        else
                        {
                            break;
                        }
                    }

                } while (amapBuffer.BytesRead != 0);

                await writeTask;
            }
            finally
            {
                if (Directory.Exists(tempPath))
                {
                    Directory.Delete(tempPath, true);
                }
            }
            return bytesWritten;
        }

        private Task StartWriteToAzure(CloudAppendBlob file, string chunkFilePath, CancellationToken token)
        {
            return Task.Run(
                async () =>
                {
                    var context = new OperationContext();
                    var options =
                        new BlobRequestOptions
                        {
                            RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(5), 10),
                            SingleBlobUploadThresholdInBytes = AppendBlobBlockSize,
                            UseTransactionalMD5 = false,
                        };
                    var condition = AccessCondition.GenerateEmptyCondition();

                    var prevChunk = 0;
                    while (true)
                    {
                        if (_chunkFilesCompleted == 0)
                        {
                            await Task.Delay(100);
                            continue;
                        }

                        await WriteCompletedFiles();

                        if (token.IsCancellationRequested)
                        {
                            await WriteCompletedFiles();
                            break;
                        }

                        await Task.Delay(100, token);
                    }

                    async Task WriteCompletedFiles()
                    {
                        while (prevChunk < _chunkFilesCompleted)
                        {
                            var chunkPath = Path.Combine(chunkFilePath, (prevChunk + 1).ToString());
                            await file.AppendFromFileAsync(chunkPath, condition, options, context);
                            File.Delete(chunkPath);
                            prevChunk++;
                        }
                    }
                }, token);
        }

        public async Task<string> CreateFileAsync(long uploadLength, string metadata, CancellationToken cancellationToken)
        {
            var fileId = Guid.NewGuid().ToString("N");
            var container = GetContainer();
            await container.CreateIfNotExistsAsync(BlobContainerPublicAccessType.Off, new BlobRequestOptions(),
                new OperationContext(), cancellationToken);

            var file = container.GetAppendBlobReference(fileId);
            file.Metadata.Add(AzureMetadata.UploadLength, uploadLength.ToString());
            file.Metadata.Add(AzureMetadata.Metadata, metadata);

            await file.CreateOrReplaceAsync(AccessCondition.GenerateIfNotExistsCondition(), new BlobRequestOptions(),
                new OperationContext(), cancellationToken);

            return fileId;
        }

        public Task<bool> FileExistAsync(string fileId, CancellationToken cancellationToken)
        {
            return GetAppendBlobReference(fileId).ExistsAsync(new BlobRequestOptions(), new OperationContext(), cancellationToken);
        }

        public async Task<long?> GetUploadLengthAsync(string fileId, CancellationToken cancellationToken)
        {
            var file = await GetAppendBlobReferenceWithAttributes(fileId, cancellationToken);

            file.Metadata.TryGetValue(AzureMetadata.UploadLength, out var lengthString);
            return string.IsNullOrEmpty(lengthString) ? (long?)null : long.Parse(lengthString);
        }

        public async Task<string> GetUploadMetadataAsync(string fileId, CancellationToken cancellationToken)
        {
            var file = await GetAppendBlobReferenceWithAttributes(fileId, cancellationToken);
            return file.Metadata.ContainsKey("Metadata") ? file.Metadata["Metadata"] : null;
        }

        public async Task<long> GetUploadOffsetAsync(string fileId, CancellationToken cancellationToken)
        {
            var file = await GetAppendBlobReferenceWithAttributes(fileId, cancellationToken);
            return file.Properties.Length == -1 ? 0 : file.Properties.Length;
        }

        public Task<ITusFile> GetFileAsync(string fileId, CancellationToken cancellationToken)
        {
            return Task.FromResult((ITusFile)new TusAzureFile(fileId, GetContainer()));
        }

        private CloudAppendBlob GetAppendBlobReference(string fileId)
        {
            return GetContainer().GetAppendBlobReference(fileId);
        }

        private async Task<CloudAppendBlob> GetAppendBlobReferenceWithAttributes(string fileId,
            CancellationToken cancellationToken)
        {
            var file = GetAppendBlobReference(fileId);
            await file.FetchAttributesAsync(AccessCondition.GenerateIfExistsCondition(), new BlobRequestOptions(),
                new OperationContext(), cancellationToken);

            return file;
        }

        private CloudBlobContainer GetContainer()
        {
            return _client.Value.GetContainerReference(_containerName);
        }
    }
}

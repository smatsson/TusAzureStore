using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using tusdotnet.Interfaces;
using TuzAzureStore.Helpers;
using TuzAzureStore.Models;

namespace TuzAzureStore.Stores
{
#warning TODO: Investigate and possibly use ConfigureAwait = false
    public class TusAzureBlobStore : ITusStore, ITusCreationStore
    {
        private readonly Lazy<CloudBlobClient> _client;
        private readonly string _containerName;

        /*
         * The size of an append blob block size (4 MB).
         * This is the optimal batch size to send to Azure as this will minimize the number 
         * of blocks being created for a single file resulting in faster reads later on.
         */
        private const int AppendBlobBlockSize = 4194304;

        public TusAzureBlobStore(string connectionString, string containerName)
        {
            _client = new Lazy<CloudBlobClient>(() => AzureStorageClientFactory.GetBlobClient(connectionString));
            _containerName = containerName;
        }

        public async Task<long> AppendDataAsync(string fileId, Stream stream, CancellationToken cancellationToken)
        {
            var file = GetContainer().GetAppendBlobReference(fileId);
            await file.FetchAttributesAsync();

            var uploadLength = long.Parse(file.Metadata["UploadLength"]);
            var fileLength = file.Properties.Length == -1 ? 0 : file.Properties.Length;

            if (uploadLength == fileLength)
            {
                return 0;
            }

            var bytesWritten = 0L;

            var amapBuffer = new AmapStreamReader(AppendBlobBlockSize, stream);

            do
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    break;
                }

#warning TODO Read data and save it until we have AppendBlobBlockSize and THEN send it to Azure. 
#warning This will optimize performance for both our send and for reading from Azure later on

                await amapBuffer.Read(cancellationToken);

                if (amapBuffer.BytesRead == 0)
                {
                    break;
                }

                await file.AppendFromByteArrayAsync(amapBuffer.Data, 0, amapBuffer.BytesRead);

                bytesWritten += amapBuffer.BytesRead;

                if (amapBuffer.LoadAbortedException != null)
                {
                    throw amapBuffer.LoadAbortedException;
                }

                if (amapBuffer.LoadAborted)
                {
                    break;
                }

            } while (amapBuffer.BytesRead != 0);

            return bytesWritten;
        }

        public async Task<string> CreateFileAsync(long uploadLength, string metadata, CancellationToken cancellationToken)
        {
            var fileId = Guid.NewGuid().ToString("N");
            var container = GetContainer();
            await container.CreateIfNotExistsAsync();

            var file = container.GetAppendBlobReference(fileId);
#warning Replace with constants
            file.Metadata.Add("UploadLength", uploadLength.ToString());
            file.Metadata.Add("Metadata", metadata);

            await file.CreateOrReplaceAsync();

            return fileId;
        }

        public async Task<bool> FileExistAsync(string fileId, CancellationToken cancellationToken)
        {
            var file = GetContainer().GetAppendBlobReference(fileId);
            return await file.ExistsAsync();
        }

        public async Task<long?> GetUploadLengthAsync(string fileId, CancellationToken cancellationToken)
        {
            var file = GetContainer().GetAppendBlobReference(fileId);
            await file.FetchAttributesAsync();

            var lengthString = file.Metadata.ContainsKey("UploadLength") ? file.Metadata["UploadLength"] : null;
            return string.IsNullOrEmpty(lengthString) ? (long?)null : long.Parse(lengthString);
        }

        public async Task<string> GetUploadMetadataAsync(string fileId, CancellationToken cancellationToken)
        {
            var file = GetContainer().GetAppendBlobReference(fileId);
            await file.FetchAttributesAsync();
            return file.Metadata.ContainsKey("Metadata") ? file.Metadata["Metadata"] : null;
        }

        public async Task<long> GetUploadOffsetAsync(string fileId, CancellationToken cancellationToken)
        {
            var file = GetContainer().GetAppendBlobReference(fileId);
            await file.FetchAttributesAsync();
            return file.Properties.Length == -1 ? 0 : file.Properties.Length;
        }

        private CloudBlobContainer GetContainer()
        {
            return _client.Value.GetContainerReference(_containerName);
        }
    }
}

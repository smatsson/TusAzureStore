//using System;
//using System.Collections.Generic;
//using System.IO;
//using System.Threading;
//using System.Threading.Tasks;
//using Microsoft.WindowsAzure.Storage.File;
//using tusdotnet.Interfaces;
//using tusdotnet.Models;
//using TuzAzureStore.Helpers;

//namespace TuzAzureStore.Stores
//{
//    public class TusAzureFileStore : ITusStore, ITusCreationStore
//    {
//        private readonly string _shareName;
//        private readonly Lazy<CloudFileClient> _client;
//        // Number of bytes to read at the time from the input stream.
//        // The lower the value, the less data needs to be re-submitted on errors.
//        // However, the lower the value, the slower the operation is. 51200 = 50 KB.
//        #warning Increase this value or let the dev specify it in an options object
//        private const int ByteChunkSize = 5120000;

//        public TusAzureFileStore(string connectionString, string shareName)
//        {
//            _shareName = shareName;
//            _client = new Lazy<CloudFileClient>(() => AzureStorageClientFactory.GetFileClient(connectionString));
//        }

//        public async Task<long> AppendDataAsync(string fileId, Stream stream, CancellationToken cancellationToken)
//        {
//            var share = _client.Value.GetShareReference(_shareName);
//            if (!await share.ExistsAsync())
//            {
//                return 0;
//            }

//            var file = share.GetRootDirectoryReference().GetFileReference(fileId);

//            var uploadLength = await GetUploadLengthAsync(fileId, cancellationToken);
//            var fileLength = file.Properties.Length == -1 ? 0 : file.Properties.Length;

//            if (uploadLength == fileLength)
//            {
//                return 0;
//            }

//            var bytesWritten = 0L;
//            // Use null as size to open an existing file instead of creating a new one
//            using (var cloudStream = await file.OpenWriteAsync(null))
//            {
//                var breakAfterWrite = false;
//                var bytesRead = int.MinValue;
//                do
//                {
//                    if (cancellationToken.IsCancellationRequested)
//                    {
//                        break;
//                    }

//                    var buffer = new byte[ByteChunkSize];

//                    try
//                    {
//                        bytesRead = await stream.ReadAsync(buffer, 0, ByteChunkSize, cancellationToken);
//                    }
//                    catch (IOException)
//                    {
//                        breakAfterWrite = true;
//                    }

//                    fileLength += bytesRead;

//                    if (fileLength > uploadLength)
//                    {
//                        throw new TusStoreException(
//                            $"Stream contains more data than the file's upload length. Stream data: {fileLength}, upload length: {uploadLength}.");
//                    }

//                    await cloudStream.WriteAsync(buffer, 0, bytesRead, cancellationToken);
//                    await cloudStream.FlushAsync(cancellationToken);

//                    bytesWritten += bytesRead;

//                    if (breakAfterWrite)
//                    {
//                        break;
//                    }

//                } while (bytesRead != 0);
//            }

//            return bytesWritten;
//        }

//        public async Task<bool> FileExistAsync(string fileId, CancellationToken cancellationToken)
//        {
//            var share = _client.Value.GetShareReference(_shareName);
//            var exists = await share.ExistsAsync();
//            if (!exists)
//            {
//                return false;
//            }

//            return await share.GetRootDirectoryReference().GetFileReference(fileId).ExistsAsync();
//        }

//        public async Task<long?> GetUploadLengthAsync(string fileId, CancellationToken cancellationToken)
//        {
//            var share = _client.Value.GetShareReference(_shareName);
//            var exists = await share.ExistsAsync();
//            if (!exists)
//            {
//#warning Create share instead of returning null?
//                return null;
//            }

//            var file = share.GetRootDirectoryReference().GetFileReference($"{fileId}.uploadlength");
//            var uploadLength = await file.DownloadTextAsync();

//            return string.IsNullOrEmpty(uploadLength) ? (long?)null : long.Parse(uploadLength);
//        }

//        public async Task<long> GetUploadOffset(string fileId, CancellationToken cancellationToken)
//        {
//            var share = _client.Value.GetShareReference(_shareName);
//            var exists = await share.ExistsAsync();
//            if (!exists)
//            {
//#warning Create share instead of returning null?
//                return 0;
//            }

//            var file = share.GetRootDirectoryReference().GetFileReference(fileId);
//            var fileLength = file.Properties.Length;
//            return fileLength == -1 ? 0 : fileLength;
//        }

//        public async Task<string> CreateFileAsync(long uploadLength, string metadata, CancellationToken cancellationToken)
//        {
//            var fileId = Guid.NewGuid().ToString("N");
//            var share = _client.Value.GetShareReference(_shareName);
//            await share.CreateIfNotExistsAsync();

//            var root = share.GetRootDirectoryReference();
//            var file = root.GetFileReference(fileId);
//            var uploadLengthFile = root.GetFileReference($"{fileId}.uploadlength");
//            var meta = root.GetFileReference($"{fileId}.metadata");

//            var tasks = new List<Task>
//            {
//                file.CreateAsync(uploadLength),
//                uploadLengthFile.UploadTextAsync(uploadLength.ToString()),
//                meta.UploadTextAsync(metadata)
//            };

//            await Task.WhenAll(tasks);

//            return fileId;
//        }

//        public async Task<string> GetUploadMetadataAsync(string fileId, CancellationToken cancellationToken)
//        {
//            var file = _client.Value.GetShareReference(_shareName).GetRootDirectoryReference().GetFileReference($"{fileId}.metadata");
//            if (!await file.ExistsAsync())
//            {
//                return null;
//            }

//            var meta = await file.DownloadTextAsync();
//            return meta;
//        }
//    }
//}

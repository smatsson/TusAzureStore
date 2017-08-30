using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using tusdotnet.Interfaces;
using tusdotnet.Models;
using TusAzureStore.Constants;

namespace TusAzureStore.Models
{
    public class TusAzureFile : ITusFile
    {
        private readonly CloudBlobContainer _container;

        internal TusAzureFile(string id, CloudBlobContainer container)
        {
            Id = id;
            _container = container;
        }

        public Task<Stream> GetContentAsync(CancellationToken cancellationToken)
        {
            return _container.GetAppendBlobReference(Id).OpenReadAsync(AccessCondition.GenerateEmptyCondition(),
                new BlobRequestOptions(), new OperationContext(), cancellationToken);
        }

        public async Task<Dictionary<string, Metadata>> GetMetadataAsync(CancellationToken cancellationToken)
        {
            var file = _container.GetAppendBlobReference(Id);
            await file.FetchAttributesAsync(AccessCondition.GenerateEmptyCondition(), new BlobRequestOptions(),
                new OperationContext(), cancellationToken);

            return Metadata.Parse(file.Metadata.ContainsKey(AzureMetadata.Metadata)
                ? file.Metadata[AzureMetadata.Metadata]
                : null);
        }

        public string Id { get; }
    }
}

using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.File;

namespace TuzAzureStore.Helpers
{
    internal static class AzureStorageClientFactory
    {
        private static readonly Dictionary<Type, object> FakeClients = new Dictionary<Type, object>();

        // Dev storage -> UseDevelopmentStorage=true
        public static CloudFileClient GetFileClient(string connectionString)
        {
            if (FakeClients.ContainsKey(typeof(CloudFileClient)))
            {
                return (CloudFileClient) FakeClients[typeof(CloudFileClient)];
            }

            var account = CloudStorageAccount.Parse(connectionString);
            return account.CreateCloudFileClient();
        }

        public static CloudBlobClient GetBlobClient(string connectionString)
        {
            if (FakeClients.ContainsKey(typeof(CloudBlobClient)))
            {
                return (CloudBlobClient) FakeClients[typeof(CloudBlobClient)];
            }

            var account = CloudStorageAccount.Parse(connectionString);
            return account.CreateCloudBlobClient();
        }

        public static void Set(CloudFileClient fake)
        {
            FakeClients[fake.GetType()] = fake;
        }

        public static void Reset<T>()
        {
            FakeClients.Remove(typeof(T));
        }
    }
}

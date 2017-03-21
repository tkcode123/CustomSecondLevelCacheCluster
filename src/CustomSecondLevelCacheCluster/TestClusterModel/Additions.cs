using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Telerik.OpenAccess;
using Telerik.OpenAccess.Metadata;

namespace TestClusterModel
{
    public partial class TestClusterModelContext
    {
        private TestClusterModelContext(string connectionId, BackendConfiguration be)
            : base(connectionId, be, metadataContainer)
        { }

        private static BackendConfiguration cachedBackendConfiguration;
        private static string cachedConnectionId;

        /// <summary>
        /// This method creates a new context using the defined transport method for L2 cache eviction control.
        /// </summary>
        /// <param name="connectionId">Name of the connection string to be used.</param>
        /// <param name="transport">Transport method class name to be used. This class will be instantiated via reflection.</param>
        /// <param name="multicastAddress">{user}:{password}@{host}:{port}. For socket based implementations can be something like "224.1.1.1:444".</param>
        /// <param name="localPath">Any value you want. For RabbitMQ will be used as the queue name.</param>
        /// <returns></returns>
        public static TestClusterModelContext Create(string connectionId, string transport, string multicastAddress = null, string localPath = null)
        {
            if (string.Equals(connectionId, cachedConnectionId) == false)
            {
                cachedBackendConfiguration = new BackendConfiguration();
                cachedBackendConfiguration.SecondLevelCache.Enabled = true;
                if (string.IsNullOrWhiteSpace(transport) == false)
                {
                    cachedBackendConfiguration.SecondLevelCache.Synchronization.Enabled = true;
                    cachedBackendConfiguration.SecondLevelCache.Synchronization.Name = transport;
                    cachedBackendConfiguration.SecondLevelCache.Synchronization.MulticastAddress = multicastAddress;
                    cachedBackendConfiguration.SecondLevelCache.Synchronization.Localpath = localPath;
                }
                cachedConnectionId = connectionId;
            }
            return new TestClusterModelContext(cachedConnectionId, cachedBackendConfiguration);
        }
    }
}

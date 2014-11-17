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
        {

        }

        private static BackendConfiguration cachedBackendConfiguration;
        private static string cachedConnectionId;

        public static TestClusterModelContext Create(string connectionId, string transport)
        {
            if (string.Equals(connectionId, cachedConnectionId) == false)
            {
                cachedBackendConfiguration = new BackendConfiguration();
                cachedBackendConfiguration.SecondLevelCache.Enabled = true;
                if (string.IsNullOrWhiteSpace(transport) == false)
                {
                    cachedBackendConfiguration.SecondLevelCache.Synchronization.Enabled = true;
                    cachedBackendConfiguration.SecondLevelCache.Synchronization.Name = transport;
                    //cachedBackendConfiguration.SecondLevelCache.Synchronization.MulticastAddress = "224.1.1.1:444";
                    cachedBackendConfiguration.SecondLevelCache.Synchronization.Localpath = "a value that you can interpret";
                }
                cachedConnectionId = connectionId;
            }
            return new TestClusterModelContext(cachedConnectionId, cachedBackendConfiguration);
        }
    }
}

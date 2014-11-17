using System;
using Telerik.OpenAccess.Cluster;

namespace CustomSecondLevelCacheClusterTransport
{
    public abstract class SecondLevelCacheClusterTransportBase : OpenAccessClusterTransport
    {
        protected OpenAccessClusterMsgHandler handler;
        protected IOpenAccessClusterTransportLog log;
        protected string multicastAddress;
        protected int multicastPort;
        protected volatile bool closed;
        protected readonly byte[] localIdentifier;

        public SecondLevelCacheClusterTransportBase()
        {
            // delay everything else until Init is called!
            this.localIdentifier = Guid.NewGuid().ToByteArray();
        }

        // set from bc.SecondLevelCache.Synchronization.MulticastAddress = "224.1.1.1:444";
        public string Multicastaddr
        {
            get { return multicastAddress + ":" + multicastPort; }
            set
            {
                int pos = value.IndexOf(':');
                multicastAddress = value.Substring(0, pos);
                multicastPort = Int32.Parse(value.Substring(pos + 1));
            }
        }

        // set from bc.SecondLevelCache.Synchronization.Localpath = "a value that you can interpret"
        public string Localpath { get; set; }

        // set from bc.SecondLevelCache.Synchronization.AdministrationQueue = "another value that you can interpret"
        public string AdministrationQueue { get; set; }

        protected bool SentByMe(byte[] received)
        {
            for (int i = 0; i < this.localIdentifier.Length; i++)
            {
                if (received[i] != this.localIdentifier[i])
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Maximum message size in bytes
        /// </summary>
        public abstract int MaxMessageSize { get; }

        /// <summary>
        /// Free all resources, called when Database is closed.
        /// </summary>
        public abstract void Close();
       
        /// <summary>
        /// Initializes transport after all properties have been set.
        /// </summary>
        /// <param name="msgHandler">The callback to use when messages are received</param>
        /// <param name="serverName">The connection id.</param>
        /// <param name="identifier">The identifier of the performance counters.</param>
        /// <param name="log">Logging instance</param>
        public abstract void Init(OpenAccessClusterMsgHandler msgHandler, string serverName, string identifier, IOpenAccessClusterTransportLog log);
                
        /// <summary>
        /// Distributes the message to all other nodes in the cluster by using a reliable, ordered 
        /// protocol. The receiving nodes must call HandleMessage for the data received.
        /// </summary>
        /// <param name="buffer">Message to send</param>
        public abstract void SendMessage(byte[] buffer);
    }
}

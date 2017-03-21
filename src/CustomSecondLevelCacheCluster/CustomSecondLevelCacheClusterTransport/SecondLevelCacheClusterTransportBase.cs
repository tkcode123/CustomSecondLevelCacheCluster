using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using Telerik.OpenAccess.Cluster;

namespace CustomSecondLevelCacheClusterTransport
{
    public abstract class SecondLevelCacheClusterTransportBase : OpenAccessClusterTransport, IDisposable
    {
        protected OpenAccessClusterMsgHandler handler;
        protected IOpenAccessClusterTransportLog log;
        protected string multicastUser;
        protected string multicastPassword;
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
            get
            {
                string address = multicastAddress + ":" + multicastPort;
                string credentials = string.Join(":", new[] { multicastUser, multicastPassword }.Where(_ => !string.IsNullOrEmpty(_)));
                if (!string.IsNullOrEmpty(credentials))
                {
                    address = credentials + "@" + address;
                }

                return address;
            }
            set
            {
                int pos = value.LastIndexOf(':');
                multicastPort = Int32.Parse(value.Substring(pos + 1));
                multicastAddress = value.Substring(0, pos);
                if (!string.IsNullOrEmpty(multicastAddress) && multicastAddress.Contains("@"))
                {
                    string credentials = multicastAddress.Substring(0, multicastAddress.LastIndexOf("@"));
                    if (!string.IsNullOrEmpty(credentials))
                    {
                        multicastUser = credentials;
                        if (credentials.Contains(":"))
                        {
                            multicastUser = credentials.Substring(0, credentials.IndexOf(":"));
                            multicastPassword = credentials.Substring(multicastUser.Length + 1);
                        }
                    }
                    multicastAddress = multicastAddress.Substring(multicastAddress.LastIndexOf("@") + 1);
                }
            }
        }

        // set from bc.SecondLevelCache.Synchronization.Localpath = "a value that you can interpret"
        public string Localpath { get; set; }

        // set from bc.SecondLevelCache.Synchronization.AdministrationQueue = "another value that you can interpret"
        public string AdministrationQueue { get; set; }

        private static bool SentByMe(byte[] received, byte[] me)
        {
            for (int i = 0; i < me.Length; i++)
            {
                if (received[i] != me[i])
                    return false;
            }
            return true;
        }

        public static bool ReceiveAll(Socket socket, byte[] target, int offset, int reqLength)
        {
            try
            {
                while (reqLength > 0)
                {
                    SocketError error;
                    int i = socket.Receive(target, offset, reqLength, SocketFlags.None, out error);
                    if (error != SocketError.Success)
                        return false;
                    offset += i;
                    reqLength -= i;
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        public static bool SendAll(Socket socket, byte[] target, int offset, int reqLength)
        {
            try
            {
                while (reqLength > 0)
                {
                    SocketError error;
                    int i = socket.Send(target, offset, reqLength, SocketFlags.None, out error);
                    if (error != SocketError.Success)
                        return false;
                    offset += i;
                    reqLength -= i;
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        public int HeaderLength { get { return this.localIdentifier.Length + 4; } }

        public static int GetLength(List<ArraySegment<byte>> list)
        {
            return list != null ? list.Sum(x => x.Count) : 0;
        }

        public int ReadHeader(byte[] buffer, out OpCode operation)
        {
            if (buffer == null || buffer.Length < (this.localIdentifier.Length + 4))
                throw new ArgumentException("Buffer to small");
            int tmp = BitConverter.ToInt32(buffer, this.localIdentifier.Length);
            operation = (OpCode)(byte)(tmp & 0xff);
            if (SentByMe(buffer, this.localIdentifier))
                operation |= OpCode.SentByMe;
            return (tmp >> 8);
        }

        protected List<ArraySegment<byte>> PrepareSending(byte[] raw, OpCode operation)
        {
            var ret = new List<ArraySegment<byte>>(3);
            ret.Add(new ArraySegment<byte>(this.localIdentifier));
            int tmp = (raw == null) ? 0 : raw.Length;
            tmp = (tmp << 8) | (byte)(operation & ~OpCode.SentByMe);
            ret.Add(new ArraySegment<byte>(BitConverter.GetBytes(tmp)));
            if (raw != null)
                ret.Add(new ArraySegment<byte>(raw));

            return ret;
        }

        [Flags]
        public enum OpCode : byte
        {
            Hello = 1,
            Welcome = 2, 
            Evict = 4,

            SentByMe = 128
        }

        protected void SendBase(byte[] buffer, Socket socket, OpCode code)
        {
            var toSend = PrepareSending(buffer, code);
            var length = GetLength(toSend);
            log.LogInformation("Sending {0} bytes", length);

            lock (this)
            {
                if (closed == false && socket != null)
                {
                    SocketError error;
                    if (socket.Send(toSend, SocketFlags.None, out error) != length)
                        closed = true;
                    else
                        closed = error != SocketError.Success;
                }
                if (closed)
                {
                    throw new InvalidOperationException("Cache Communication Broken");
                }
            }
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

        public static Socket SafeClose(Socket s)
        {
            if (s != null)
            {
                try
                {
                    s.Shutdown(SocketShutdown.Both); s.Close();
                }
                catch
                {
                }
                s = null;
            }
            return s;
        }

        #region IDisposable Members

        ~SecondLevelCacheClusterTransportBase()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected abstract void Dispose(bool disposing);

        #endregion
    }
}

using System.Net.Sockets;
using System.Net;
using System.Threading;
using Telerik.OpenAccess.Cluster;
using System.IO;
using System;
using System.Collections.Generic;

namespace CustomSecondLevelCacheClusterTransport
{
    public class RdmClusterTransport : OpenAccessClusterTransport
    {
        public static int Counter = 0;

        private OpenAccessClusterMsgHandler handler;
        private IOpenAccessClusterTransportLog log;
        private string multicastAddress;
        private int multicastPort;
        private string localpath;
        private Socket senderSocket;
        private Socket receiverSocket;
        private volatile bool closed;
        private readonly byte[] localIdentifier;

        public RdmClusterTransport()
        {
            // delay everything until Init is called!
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
        public string Localpath
        {
            get { return localpath; }
            set { localpath = value; }
        }

        public int MaxMessageSize
        {
            get { return 65000; }
        }

        public void Init(OpenAccessClusterMsgHandler messageHandler, string serverName, string identifier, IOpenAccessClusterTransportLog log)
        {
            this.handler = messageHandler;
            this.log = log;

            if (string.IsNullOrWhiteSpace(this.multicastAddress) || this.multicastPort <= 0)
                throw new ArgumentException("Missing MulticastAddress setting.");
            IPAddress ip = IPAddress.Parse(multicastAddress);
            IPEndPoint ipep = new IPEndPoint(ip, multicastPort);

            // create send socket
            senderSocket = new Socket(AddressFamily.InterNetwork, SocketType.Rdm, (ProtocolType)113);

            // bind socket to network interface IP address ANY 
            //sendSocket.Bind(new IPEndPoint(IPAddress.Any, 0));

            // connect socket to multicast address group
            senderSocket.Connect(ipep);


            receiverSocket = new Socket(AddressFamily.InterNetwork, SocketType.Rdm, (ProtocolType)113);
            // bind socket to multicast group
            receiverSocket.ExclusiveAddressUse = false;
            receiverSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            receiverSocket.Bind(ipep);
            receiverSocket.Listen(10);
            receiverSocket.BeginAccept(OnAccept, null);

        }

        public void SendMessage(byte[] buffer)
        {
            var dup = new byte[buffer.Length + localIdentifier.Length];
            Buffer.BlockCopy(localIdentifier, 0, dup, 0, localIdentifier.Length);
            Buffer.BlockCopy(buffer, 0, dup, localIdentifier.Length, buffer.Length);

            log.LogInformation("Sending {0} bytes", dup.Length);

            lock (this)
            {
                if (closed == false && senderSocket != null)
                {
                    if (senderSocket.Send(dup, dup.Length, SocketFlags.None) != dup.Length)
                        throw new InvalidOperationException("Socket did not send all bytes.");
                }
            }
        }

        public void Close()
        {
            lock (this)
            {
                closed = true;
                if (senderSocket != null)
                {
                    try { senderSocket.Shutdown(SocketShutdown.Send); senderSocket.Close(); senderSocket = null; }
                    catch { }
                }
                if (receiverSocket != null)
                {
                    try { receiverSocket.Shutdown(SocketShutdown.Receive); receiverSocket.Close(); receiverSocket = null; }
                    catch { }
                }
            }
            
            handler = null;
        }

        private void OnAccept(IAsyncResult result)
        {
            try
            {
                if (result.IsCompleted)
                {
                    var p = new Partner(this.receiverSocket.EndAccept(result), this);

                    ThreadPool.QueueUserWorkItem(p.Receive);

                    receiverSocket.BeginAccept(OnAccept, null);
                }
            }
            catch
            {
                Close();
            }
        }

        private bool SentByMe(byte[] received)
        {
            for (int i = 0; i < this.localIdentifier.Length; i++)
            {
                if (received[i] != this.localIdentifier[i])
                    return false;
            }
            return true;
        }

        class Partner
        {
            internal readonly Socket socket;
            internal readonly RdmClusterTransport handler;

            internal Partner(Socket s, RdmClusterTransport h)
            {
                socket = s;
                handler = h;
            }

            public void Receive(object x)
            {
                var tmp = new byte[handler.MaxMessageSize];
                SocketError err;
                int len = socket.Receive(tmp, 0, tmp.Length, SocketFlags.None, out err);
                if (err != SocketError.Success)
                {
                    try
                    { socket.Close(); socket.Dispose(); }
                    catch { }
                    return;
                }
                if (handler.SentByMe(tmp) == false)
                {
                    handler.log.LogInformation("Received {0} bytes", tmp.Length);
                    handler.handler.HandleMessage(new MemoryStream(tmp, handler.localIdentifier.Length, 
                                                                        len - handler.localIdentifier.Length, false));
                }
                else
                {
                    handler.log.LogInformation("Got my own eviction");
                }
                ThreadPool.QueueUserWorkItem(this.Receive);
            }

        }
    }
}
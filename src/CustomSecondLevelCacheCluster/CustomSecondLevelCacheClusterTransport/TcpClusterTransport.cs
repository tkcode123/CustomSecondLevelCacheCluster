using System.Net.Sockets;
using System.Net;
using System.Threading;
using Telerik.OpenAccess.Cluster;
using System.IO;
using System;

namespace CustomSecondLevelCacheClusterTransport
{
    public class TcpClusterTransport : OpenAccessClusterTransport
    {
        public static int Counter = 0;

        private OpenAccessClusterMsgHandler handler;
        private IOpenAccessClusterTransportLog log;
        private string multicastAddress;
        private int multicastPort;
        private string localpath;
        private Socket socket;
        private Thread receiverThread;
        private volatile bool closed;
        private readonly byte[] localIdentifier;

        public TcpClusterTransport()
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

            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(ipep);

            receiverThread = new Thread(new ThreadStart(ReceiveLoop));
            receiverThread.Name = "Cache Eviction Listener";
            receiverThread.Start();
        }

        public void SendMessage(byte[] buffer)
        {
            var dup = new byte[buffer.Length + localIdentifier.Length];
            Buffer.BlockCopy(localIdentifier, 0, dup, 0, localIdentifier.Length);
            Buffer.BlockCopy(buffer, 0, dup, localIdentifier.Length, buffer.Length);

            log.LogInformation("Sending {0} bytes", dup.Length);

            lock (this)
            {
                if (socket.Send(dup, dup.Length, SocketFlags.None) != dup.Length)
                    throw new InvalidOperationException("Socket did not send all bytes.");
            }
        }

        public void Close()
        {
            closed = true;
            if (socket != null)
            {
                socket.Shutdown(SocketShutdown.Both);
                socket.Close();
            }
            if (receiverThread != null)
            {
                receiverThread.Abort();
                if (receiverThread.Join(2000))
                {
                    receiverThread = null;
                    socket.Dispose();
                }
            }
            handler = null;
        }

        private void ReceiveLoop()
        {
            try
            {
                byte[] b = new byte[MaxMessageSize + localIdentifier.Length];
                while (closed == false)
                {
                    try
                    {
                        // TODO: size of the buffer must be checked, maybe receive in a loop!
                        SocketError error;
                        int i = socket.Receive(b, 0, b.Length, SocketFlags.None, out error);
                        if (error == SocketError.TimedOut)
                            continue;
                        if (error == SocketError.Interrupted || error == SocketError.OperationAborted)
                            break;
                        if (i > this.localIdentifier.Length)
                        {
                            if (SentByMe(b) == false)
                            {
                                handler.HandleMessage(new MemoryStream(b, this.localIdentifier.Length, i - this.localIdentifier.Length, false));
                                Interlocked.Increment(ref Counter);
                                log.LogInformation("Received {0} bytes", i);
                            }
                            else
                            {
                                log.LogInformation("Got my own eviction");
                            }
                        }
                    }
                    catch (SocketException e)
                    {
                        log.LogWarning("Receiver got exception {0}", e.Message);
                    }
                }
            }
            finally // TODO Error handling and restart!
            {
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
    }
}
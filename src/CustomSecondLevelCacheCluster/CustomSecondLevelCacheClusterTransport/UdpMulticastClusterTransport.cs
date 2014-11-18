using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Telerik.OpenAccess.Cluster;

namespace CustomSecondLevelCacheClusterTransport
{
    public class UdpMulticastClusterTransport : SecondLevelCacheClusterTransportBase
    {
        public static int Counter = 0;

        private Socket senderSocket;
        private Socket receiverSocket;
        private Thread receiverThread;

        public UdpMulticastClusterTransport()
        {
            this.Multicastaddr = "224.1.1.1:444";
        }
      
        public override int MaxMessageSize
        {
            get { return 65000; }
        }

        public override void Init(OpenAccessClusterMsgHandler messageHandler, string serverName, string identifier, IOpenAccessClusterTransportLog log)
        {
            this.handler = messageHandler;
            this.log = log;

            if (string.IsNullOrWhiteSpace(this.multicastAddress) || this.multicastPort <= 0)
                throw new ArgumentException("Missing MulticastAddress setting.");
            IPAddress ip = IPAddress.Parse(multicastAddress);
            IPEndPoint ipep = new IPEndPoint(ip, multicastPort);

            senderSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            senderSocket.SetSocketOption(SocketOptionLevel.Udp, SocketOptionName.NoDelay, 1);
            senderSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, 1);
            senderSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.AddMembership, new MulticastOption(ip));
            senderSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.MulticastTimeToLive, 1);

            senderSocket.Connect(ipep);

            receiverSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            receiverSocket.ReceiveTimeout = 1000;
            IPEndPoint ipepRecv = new IPEndPoint(IPAddress.Any, multicastPort);
            IPAddress ipRecv = IPAddress.Parse(multicastAddress);
            receiverSocket.SetSocketOption(SocketOptionLevel.Udp, SocketOptionName.NoDelay, 1);
            receiverSocket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, 1);
            receiverSocket.SetSocketOption(SocketOptionLevel.IP, SocketOptionName.AddMembership, new MulticastOption(ipRecv, IPAddress.Any));

            receiverSocket.Bind(ipepRecv);

            receiverThread = new Thread(new ThreadStart(ReceiveLoop));
            receiverThread.Name = "Cache Eviction Listener";
            receiverThread.Start();
        }

        public override void SendMessage(byte[] buffer)
        {
            SendBase(buffer, senderSocket);
        }

        public override void Close()
        {
            closed = true;
            if (senderSocket != null)
                senderSocket.Close();
            if (receiverSocket != null)
                receiverSocket.Close();
            if (receiverThread != null)
            {
                receiverThread.Abort();
                if (receiverThread.Join(2000))
                {
                    receiverThread = null;
                    this.Dispose();
                }
            }
            handler = null;
        }

        private void ReceiveLoop()
        {           
            try
            {
                byte[] b = new byte[MaxMessageSize+HeaderLength];
                while (closed == false)
                {
                    try
                    {
                        // TODO: size of the buffer must be checked, maybe receive in a loop!
                        SocketError error;
                        int i = receiverSocket.Receive(b, 0, b.Length, SocketFlags.None, out error);
                        if (error == SocketError.TimedOut)
                            continue;
                        if (error == SocketError.Interrupted || error == SocketError.OperationAborted)
                            break;
                        if (i >= HeaderLength)
                        {
                            OpCode code;
                            int j = ReadHeader(b, out code);
                            if (j + HeaderLength != i)
                                throw new InvalidOperationException("Received different length than expected");
                            if ((code & OpCode.SentByMe) != OpCode.SentByMe)
                            {
                                handler.HandleMessage(new MemoryStream(b, HeaderLength, j, false));
                                Interlocked.Increment(ref Counter);
                                log.LogInformation("Received {0} bytes", i);
                            }
                            else
                            {
                                log.LogInformation("Got my own eviction");
                            }
                        }
                        else
                            throw new InvalidOperationException("Received message to short");
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

        #region IDisposable Members

        protected override void Dispose(bool disposing)
        {
            if (closed == false && disposing)
            {
                if (senderSocket != null)
                    senderSocket.Dispose();
                if (receiverSocket != null)
                    receiverSocket.Dispose();
                senderSocket = receiverSocket = null;
            }
        }

        #endregion
    }
}
using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Telerik.OpenAccess.Cluster;

namespace CustomSecondLevelCacheClusterTransport
{
    public class RdmClusterTransport : SecondLevelCacheClusterTransportBase
    {
        public static int Counter = 0;

        private Socket senderSocket;
        private Socket receiverSocket;

        public RdmClusterTransport()
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

        public override void SendMessage(byte[] buffer)
        {
            SendBase(buffer, senderSocket, OpCode.Evict);
        }

        public override void Close()
        {
            lock (this)
            {
                closed = true;
                senderSocket = SafeClose(senderSocket);
                receiverSocket = SafeClose(receiverSocket);
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

        private bool ReceiveFrom(Partner p)
        {
            var b = new byte[MaxMessageSize + HeaderLength];
            SocketError error;
            int i = p.socket.Receive(b, 0, b.Length, SocketFlags.None, out error);
            if (error != SocketError.Success)
            {
                return false;
            }
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
                return true;
            }
            else
            {
                log.LogInformation("Received message to short");
                return false;
            }
        }

        #region IDisposable Members

        protected override void Dispose(bool disposing)
        {
            if (closed == false && disposing)
            {
                Close();
                if (senderSocket != null)
                    senderSocket.Dispose();
                if (receiverSocket != null)
                    receiverSocket.Dispose();
            }
        }

        #endregion

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
                if (handler.ReceiveFrom(this))
                    ThreadPool.QueueUserWorkItem(this.Receive);
                else
                {
                    try { socket.Close(); socket.Dispose(); }
                    catch { }
                }
            }
        }
    }
}
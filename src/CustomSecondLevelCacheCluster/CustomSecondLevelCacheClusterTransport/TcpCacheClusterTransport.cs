using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using Telerik.OpenAccess.Cluster;

namespace CustomSecondLevelCacheClusterTransport
{
    public class TcpCacheClusterTransport : SecondLevelCacheClusterTransportBase
    {
        public static int Counter = 0;

        private Socket socket;
        private Thread receiverThread;

        public TcpCacheClusterTransport()
        {
            Multicastaddr = "127.0.0.1:9999";
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

            IPAddress ip;
            if (IPAddress.TryParse(this.multicastAddress, out ip) == false)
                throw new ArgumentException(string.Format("Unable to parse IP address {0}", this.multicastAddress));
            IPEndPoint ipep = new IPEndPoint(ip, multicastPort);

            // Create a TCP/IP  socket.
            socket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp );
            try
            {
                socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.NoDelay, 1);

                // Connect the socket to the remote endpoint
                socket.Connect(ipep);

                var greeting = Encoding.UTF8.GetBytes(string.IsNullOrEmpty(this.Localpath) ? "<Localpath>" : this.Localpath);
                SendBase(greeting, socket, OpCode.Hello);

                byte[] tmp = new byte[HeaderLength];
                if (ReceiveAll(socket, tmp, 0, tmp.Length))
                {
                    OpCode code;
                    if (ReadHeader(tmp, out code) == 0 && (code & OpCode.Welcome) != 0)
                    {
                        receiverThread = new Thread(new ThreadStart(ReceiveLoop));
                        receiverThread.Name = "Cache Eviction Listener for " + ipep.ToString();
                        receiverThread.Start();
                    }
                    else
                        Close();
                }
                else
                    Close();
            }
            catch
            {
                Close();
                throw;
            }
            if (socket == null)
                throw new InvalidOperationException("Initial Handshake with TcpCacheClusterServer failed.");
        }

        public override void SendMessage(byte[] buffer)
        {
            SendBase(buffer, socket, OpCode.Evict);
        }

        public override void Close()
        {
            closed = true;
            if (socket != null)
            {
                SafeClose(socket);
            }
            if (receiverThread != null)
            {
                receiverThread.Abort();
                if (receiverThread.Join(2000))
                {
                    receiverThread = null;
                    socket.Dispose();
                    socket = null;
                }
            }
            handler = null;
        }

        private void ReceiveLoop()
        {
            try
            {
                byte[] b = new byte[HeaderLength];
                while (closed == false)
                {
                    try
                    {
                        if (ReceiveAll(socket, b, 0, HeaderLength))
                        {
                            OpCode code;
                            int len = ReadHeader(b, out code);
                            byte[] tmp = new byte[len];
                            if (ReceiveAll(socket, tmp, 0, len))
                            {
                                if ((code & OpCode.SentByMe) == 0)
                                {
                                    handler.HandleMessage(new MemoryStream(tmp, 0, len, false));
                                    Interlocked.Increment(ref Counter);
                                    log.LogInformation("Received {0} bytes", len + HeaderLength);
                                }
                                else
                                {
                                    log.LogInformation("Got my own eviction");
                                }
                            }
                            else
                                break;
                        }
                        else
                            break;
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
            if (closed == false && disposing && socket != null)
            {
                SafeClose(socket);
                socket.Dispose();
                socket = null;
            }
        }

        #endregion
    }
}

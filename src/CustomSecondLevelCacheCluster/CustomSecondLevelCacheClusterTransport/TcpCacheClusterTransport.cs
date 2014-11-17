using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
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
            IPAddress ip = IPAddress.Parse(multicastAddress);
            IPEndPoint ipep = new IPEndPoint(ip, multicastPort);

            // Create a TCP/IP  socket.
            socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp );
            socket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.NoDelay, 1);

            // Connect the socket to the remote endpoint
            socket.Connect(ipep);           

            receiverThread = new Thread(new ThreadStart(ReceiveLoop));
            receiverThread.Name = "Cache Eviction Listener";
            receiverThread.Start();
        }

        public override void SendMessage(byte[] buffer)
        {
            List<ArraySegment<byte>> pieces = new List<ArraySegment<byte>>(3);
            pieces.Add(new ArraySegment<byte>(this.localIdentifier));
            lock (this)
            {
                var lengthBuffer = BitConverter.GetBytes(buffer.Length);
                pieces.Add(new ArraySegment<byte>(lengthBuffer));
                pieces.Add(new ArraySegment<byte>(buffer));
                int entire = buffer.Length + localIdentifier.Length + lengthBuffer.Length;
                int ret = socket.Send(pieces, SocketFlags.None);
                if (ret != entire)
                    throw new InvalidOperationException(string.Format("Cache Communication broken."));
            }
        }

        public override void Close()
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
                    socket = null;
                }
            }
            handler = null;
        }

        private void ReceiveLoop()
        {
            try
            {
                byte[] b = new byte[MaxMessageSize + localIdentifier.Length + 4];
                while (closed == false)
                {
                    try
                    {
                        if (ReceiveAll(b, 0, localIdentifier.Length + 4))
                        {
                            int len = BitConverter.ToInt32(b, localIdentifier.Length);
                            bool fromMe = SentByMe(b);
                            if (ReceiveAll(b, 0, len))
                            {
                                if (fromMe == false)
                                {
                                    handler.HandleMessage(new MemoryStream(b, 0, len, false));
                                    Interlocked.Increment(ref Counter);
                                    log.LogInformation("Received {0} bytes", len + 4 + localIdentifier.Length);
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

        private bool ReceiveAll(byte[] target, int offset, int reqLength)
        {
            while (reqLength > 0)
            {
                SocketError error;
                int i = this.socket.Receive(target, offset, reqLength, SocketFlags.None, out error);
                if (error != SocketError.Success)
                    return false;
                offset += i;
                reqLength -= i;
            }
            return true;
        }
    }
}

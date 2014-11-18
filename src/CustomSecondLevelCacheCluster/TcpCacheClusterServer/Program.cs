using CustomSecondLevelCacheClusterTransport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace TcpCacheClusterServer
{
    public class BroadcastClient
    {
        public readonly Socket ClientSocket;
        public readonly byte[] HeaderBuffer;
        // Access to definitions that the DataAccess client also uses.
        public readonly SecondLevelCacheClusterTransportBase Definition;
        // String representation of the client.
        private readonly string name;

        public BroadcastClient(Socket s, SecondLevelCacheClusterTransportBase t)
        {
            ClientSocket = s;
            Definition = t;
            HeaderBuffer = new byte[t.HeaderLength];
            name = s.RemoteEndPoint.ToString();
        }

        public override string ToString() { return name; }  
    }

    class Broadcast
    {
        private static int counter;

        public Broadcast()
        {
            Number = Interlocked.Increment(ref counter);
        }

        public byte[] Data { get; set; }
        public BroadcastClient Sender { get; set; }
        public int Number { get; private set; } 
    }

    class BroadcastTarget
    {
        public Broadcast Message { get; set; }
        public BroadcastClient Target { get; set; }
    }

    public class AsynchronousSocketListener 
    {
        // Thread signal.
        private static ManualResetEvent allDone = new ManualResetEvent(false);
        internal static SecondLevelCacheClusterTransportBase definition = new TcpCacheClusterTransport();
        private static readonly List<BroadcastClient> clients = new List<BroadcastClient>(8);

        public static void Main(string[] args)
        {
            int port = args.Length > 0 ? Int32.Parse(args[0]) : 9999;
            
            IPEndPoint localEndPoint = new IPEndPoint(IPAddress.Any, port);

            // Create a TCP/IP socket.
            using (var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                // Bind the socket to the local endpoint and listen for incoming connections.
                try
                {
                    listener.Bind(localEndPoint);
                    listener.Listen(10);

                    Console.WriteLine("Accepting cache cluster connections on {0}", listener.LocalEndPoint);

                    while (true)
                    {
                        // Set the event to nonsignaled state.
                        allDone.Reset();

                        // Start an asynchronous socket to listen for connections.
                        listener.BeginAccept(
                            new AsyncCallback(AcceptCallback),
                            listener);

                        // Wait until a connection is made before continuing.
                        allDone.WaitOne();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                }
            }
        }

        public static void AcceptCallback(IAsyncResult ar)
        {
            // Signal the main thread to continue.
            allDone.Set();

            // Get the socket that handles the client request.
            Socket listener = (Socket)ar.AsyncState;
            Socket clientSocket = listener.EndAccept(ar);

            // Create the state object.
            BroadcastClient client = new BroadcastClient(clientSocket, definition);
            lock (clients)
            {
                clients.Add(client);
                Console.WriteLine("ACCEPTED {0}", client);
            }
            clientSocket.BeginReceive(client.HeaderBuffer, 0, client.HeaderBuffer.Length, SocketFlags.None,
                                new AsyncCallback(ReadCallback), client);
        }

        public static void ReadCallback(IAsyncResult ar)
        {
            // Retrieve the state object and the handler socket
            // from the asynchronous state object.
            BroadcastClient source = (BroadcastClient)ar.AsyncState;

            // Read data from the client socket. 
            bool close = false;
            try
            {
                int bytesRead = source.ClientSocket.EndReceive(ar);
           
                if (bytesRead == source.Definition.HeaderLength)
                {
                    SecondLevelCacheClusterTransportBase.OpCode code;
                    int len = source.Definition.ReadHeader(source.HeaderBuffer, out code);
                    if (len > 0 && (code & SecondLevelCacheClusterTransportBase.OpCode.Evict) != 0)
                    {
                        var tmp = new byte[len+source.Definition.HeaderLength];
                        Buffer.BlockCopy(source.HeaderBuffer, 0, tmp, 0, source.Definition.HeaderLength);
                        if (SecondLevelCacheClusterTransportBase.ReceiveAll(source.ClientSocket, tmp, source.Definition.HeaderLength, len))
                        {
                            var bc = new Broadcast() { Data = tmp, Sender = source };
                            Console.WriteLine("RECEIVED #{0} of {1} bytes from {2}", bc.Number, tmp.Length, source);
                            ThreadPool.QueueUserWorkItem(PerformBroadcast, bc);
                        }
                        else
                        {
                            Console.WriteLine("Data truncation {0} bytes from {1}", len, source);
                            close = true;
                        }
                    }
                    else 
                    {
                        Console.WriteLine("RECEIVED {0} / {1} from {2}", code, len, source);
                    }
                }
                else
                    close = true;
            }
            catch
            {
                close = true;
            }

            if (close)
            {
                source.ClientSocket.Shutdown(SocketShutdown.Both);
                source.ClientSocket.Close();
                source.ClientSocket.Dispose();
                lock (clients)
                {
                    clients.Remove(source);
                    Console.WriteLine("DISCONNECTED {0}", source);
                }
            }
            else
            {
                source.ClientSocket.BeginReceive(source.HeaderBuffer, 0, source.HeaderBuffer.Length, SocketFlags.None,
                                                new AsyncCallback(ReadCallback), source);
            }
        }

        private static void PerformBroadcast(object bc)
        {
            Broadcast broadcast = (Broadcast)bc;
            List<BroadcastTarget> toSend;
            lock (clients)
            {
                toSend = clients.Where(x => x != broadcast.Sender)
                                .Select(x => new BroadcastTarget() { Message = broadcast, Target = x }).ToList();
            }
            foreach (var bct in toSend)
            {
                Console.WriteLine("  SENDING #{0} to {1}", broadcast.Number, bct.Target);

                try
                {
                    bct.Target.ClientSocket.BeginSend(bct.Message.Data, 0, bct.Message.Data.Length, SocketFlags.None,
                                                      new AsyncCallback(SendCallback), bct);
                }
                catch
                {
                    Shutdown(bct.Target.ClientSocket);
                }
            }
        }

        private static void SendCallback(IAsyncResult ar)
        {
            BroadcastTarget target = (BroadcastTarget)ar.AsyncState;
            bool close = false;
            try
            {
                // Completed sending the data to the remote device.
                int bytesSent = target.Target.ClientSocket.EndSend(ar);
                if (bytesSent != target.Message.Data.Length)
                {
                    Console.WriteLine("Data send problem #{0} to {1}", target.Message.Number, target.Target);
                    close = true;                    
                }
            }
            catch
            {
                close = true;
            }
            if (close)
            {
                Shutdown(target.Target.ClientSocket);
            }
        }

        private static void Shutdown(Socket socket)
        {
            try
            {
                socket.Shutdown(SocketShutdown.Both);
            }
            catch
            {
            }
        }
    }
}
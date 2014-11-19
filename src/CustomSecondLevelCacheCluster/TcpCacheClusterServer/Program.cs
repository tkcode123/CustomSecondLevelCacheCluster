using CustomSecondLevelCacheClusterTransport;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace TcpCacheClusterServer
{
    /// <summary>
    /// Represents the client in the server.
    /// </summary>
    class Partner
    {
        public readonly Socket ClientSocket;
        public readonly byte[] HeaderBuffer;
        // Access to definitions that the DataAccess client also uses.
        public readonly SecondLevelCacheClusterTransportBase Definition;
        // String representation of the client.
        private readonly string name;
        // The name that was set on the client side via Localpath property.
        private string alias;

        public Partner(Socket s, SecondLevelCacheClusterTransportBase t)
        {
            ClientSocket = s;
            Definition = t;
            HeaderBuffer = new byte[t.HeaderLength];
            alias = name = s.RemoteEndPoint.ToString();
        }

        internal void SetAlias(string a)
        {
            alias = string.Format("{0} ({1})", a, name);
        }
        public override string ToString() { return alias; }
        public int Evictions { get; set; }
    }

    /// <summary>
    /// Represents a particular message to broadcast.
    /// </summary>
    class Broadcast
    {
        private static int counter;

        public Broadcast()
        {
            Number = Interlocked.Increment(ref counter);
        }

        public byte[] Data { get; set; }
        public Partner Sender { get; set; }
        public int Number { get; private set; } 
    }

    /// <summary>
    /// Represents the action of sending one broadcast message to a particular client.
    /// </summary>
    class BroadcastTarget
    {
        public Broadcast Message { get; set; }
        public Partner Target { get; set; }
    }

    public class Program 
    {
        // Signals the ability to accept the next client.
        private static ManualResetEvent allDone = new ManualResetEvent(false);
        // Provides access to the definitions that the cache cluster clients use too.
        internal static SecondLevelCacheClusterTransportBase definition = new TcpCacheClusterTransport();
        // List of currently connected clients.
        private static readonly List<Partner> clients = new List<Partner>(8);
        // Simple termination
        private static int done = 5;

        public static void Main(string[] args)
        {
            int port = args.Length > 0 ? Int32.Parse(args[0]) : 9999;
            
            IPEndPoint localEndPoint = new IPEndPoint(IPAddress.Any, port);

            // Create a TCP/IP socket.
            using (var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp))
            {
                try
                {
                    // Bind the socket to the local endpoint and listen for incoming connections.
                    listener.Bind(localEndPoint);
                    listener.Listen(10);

                    Console.CancelKeyPress += Console_CancelKeyPress;
                    Console.WriteLine("Accepting cache cluster connections on {0}, 5x CTRL-C to end.", listener.LocalEndPoint);

                    while (done > 0)
                    {
                        // Set the event to nonsignaled state.
                        allDone.Reset();

                        // Start an asynchronous socket to listen for connections.
                        listener.BeginAccept(new AsyncCallback(AcceptCallback), listener);

                        // Wait until a connection is made before continuing.
                        allDone.WaitOne();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                }
            }
        }

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            done--;
            e.Cancel = done > 0;
            Console.WriteLine("CLIENTS", clients.Count);
            for (int i = 0; i < clients.Count; i++)
            {
                Console.WriteLine("  <{0}> Evictions={1} {2}", i, clients[i].Evictions, clients[i]);
            }
        }

        static void AcceptCallback(IAsyncResult ar)
        {
            // Get the socket that handles the client request.
            Socket listener = (Socket)ar.AsyncState;
            Socket clientSocket = listener.EndAccept(ar);

            // Signal the main thread to continue.
            allDone.Set();

            // Create the state object.
            Partner client = new Partner(clientSocket, definition);
            lock (clients)
            {
                clients.Add(client);
                Console.WriteLine("ACCEPTED {0}", client);
            }
            // Avoid Nagle algorithm for this socket, preventing delays in sending.
            clientSocket.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.NoDelay, true);

            // Start immediately to receive incoming messages.
            clientSocket.BeginReceive(client.HeaderBuffer, 0, client.HeaderBuffer.Length, SocketFlags.None,
                                      new AsyncCallback(ReadCallback), client);
        }

        static void ReadCallback(IAsyncResult ar)
        {
            // Retrieve the state object and the handler socket from the asynchronous state object.
            Partner source = (Partner)ar.AsyncState;

            bool close = false;
            try
            {
                // Read data from the client socket. 
                int bytesRead = source.ClientSocket.EndReceive(ar);
           
                // TODO Handle situations when the messages get fragmented and not all bytes are returned immediately.
                if (bytesRead == source.Definition.HeaderLength)
                {
                    SecondLevelCacheClusterTransportBase.OpCode code;
                    int len = source.Definition.ReadHeader(source.HeaderBuffer, out code);
                    if (len > definition.MaxMessageSize)
                        close = true; // A little protection must be.
                    else if (len > 0 && (code & SecondLevelCacheClusterTransportBase.OpCode.Evict) != 0)
                    {
                        var tmp = new byte[len+source.Definition.HeaderLength];
                        Buffer.BlockCopy(source.HeaderBuffer, 0, tmp, 0, source.Definition.HeaderLength);

                        // Assumption: Because the sender pushes one message, we should have all bytes received in the
                        // OS already, and we can use synchronous calls here without negative effects.
                        if (SecondLevelCacheClusterTransportBase.ReceiveAll(source.ClientSocket, tmp, source.Definition.HeaderLength, len))
                        {
                            source.Evictions++;
                            var bc = new Broadcast() { Data = tmp, Sender = source };
                            Console.WriteLine("RECEIVED #{0} of {1} bytes from {2}", bc.Number, tmp.Length, source);

                            // Let some other thread do the work of actually sending the message out.
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
                        if ((code & SecondLevelCacheClusterTransportBase.OpCode.Hello) != 0)
                        {                            
                            var nameBytes = new byte[len];
                            if (SecondLevelCacheClusterTransportBase.ReceiveAll(source.ClientSocket, nameBytes, 0, len))
                            {
                                try
                                {
                                    var name = Encoding.UTF8.GetString(nameBytes);
                                    source.SetAlias(name);
                                    Console.WriteLine("HELLO {0}", source);
                                    Buffer.BlockCopy(BitConverter.GetBytes((int)SecondLevelCacheClusterTransportBase.OpCode.Welcome), 0, source.HeaderBuffer, definition.HeaderLength-4, 4);
                                    close = SecondLevelCacheClusterTransportBase.SendAll(source.ClientSocket, source.HeaderBuffer, 0, source.HeaderBuffer.Length) == false;
                                }
                                catch
                                {
                                    close = true;
                                }
                            }
                            else
                                close = true;
                        }
                        else
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
                // Something went wrong, let's disconnect from the partner. Maybe a better handling can be done.
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
                // Immediately try to receive the next message from the same partner.
                source.ClientSocket.BeginReceive(source.HeaderBuffer, 0, source.HeaderBuffer.Length, SocketFlags.None,
                                                 new AsyncCallback(ReadCallback), source);
            }
        }

        static void PerformBroadcast(object bc)
        {
            Broadcast broadcast = (Broadcast)bc;
            List<BroadcastTarget> toSend;
            lock (clients)
            {   // Find all parties that must receive this message.
                toSend = clients.Where(x => x != broadcast.Sender) // Don't send it back to the sending partner.
                                .Select(x => new BroadcastTarget() { Message = broadcast, Target = x }).ToList();
            }
            foreach (var bct in toSend)
            {
                Console.WriteLine("  SENDING #{0} to {1}", broadcast.Number, bct.Target);

                try
                {   // Assumption: We are giving the message as one piece to the socket, and we do not need to protect
                    // against parallel writes therefore. If sending the message results in SENDING_PROBLEM, we need to 
                    // reconsider this.
                    bct.Target.ClientSocket.BeginSend(bct.Message.Data, 0, bct.Message.Data.Length, SocketFlags.None,
                                                      new AsyncCallback(SendCallback), bct);
                }
                catch
                {
                    Shutdown(bct.Target.ClientSocket);
                }
            }
        }

        static void SendCallback(IAsyncResult ar)
        {
            BroadcastTarget target = (BroadcastTarget)ar.AsyncState;
            bool close = false;
            try
            {
                // Completed sending the data to the remote device.
                int bytesSent = target.Target.ClientSocket.EndSend(ar);
                if (bytesSent != target.Message.Data.Length)
                {
                    if (bytesSent != 0) // Could it be a concurrent disconnect?
                        Console.WriteLine("    SENDING_PROBLEM #{0} to {1}", target.Message.Number, target.Target);
                    close = true;                    
                }
            }
            catch
            {
                close = true;
            }
            if (close)
            {   // Forcing the receiving async operation to fail, which in turn eliminates our state for this partner.
                Shutdown(target.Target.ClientSocket);
            }
        }

        // Shutdown without problems.
        static void Shutdown(Socket socket)
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
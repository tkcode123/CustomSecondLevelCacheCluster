using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace TcpClusterServer
{
    class Program
    {
        static void Main(string[] args)
        {
            int port = args.Length > 0 ? Int32.Parse(args[0]) : 9999;
            TcpListener listener = new TcpListener(IPAddress.Any, port);
            listener.Start();

            Console.WriteLine(string.Format("Server started at {0}:{1}", listener.LocalEndpoint, port));

            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            // Bind the socket to the address and port.
            socket.Bind(new IPEndPoint(IPAddress.Any, port));

            // Start listening.
            socket.Listen(5);

            // Set up the callback to be notified when somebody requests
            // a new connection.
            socket.BeginAccept(OnConnectRequest, socket);

            Console.WriteLine("Quit--->Return");
            Console.ReadLine();
        }

        // This is the method that is called when the socket recives a request
        // for a new connection.
        private static void OnConnectRequest(IAsyncResult result)
        {
            // Get the socket (which should be this listener's socket) from
            // the argument.
            Socket sock = (Socket)result.AsyncState;
            // Create a new client connection, using the primary socket to
            // spawn a new socket.
            Connection newConn = new Connection(sock.EndAccept(result));
            // Tell the listener socket to start listening again.
            sock.BeginAccept(OnConnectRequest, sock);
        }
    }

    public class Connection
    {
        private Socket sock;
        // Pick whatever encoding works best for you.  Just make sure the remote 
        // host is using the same encoding.
        private Encoding encoding = Encoding.UTF8;
        private byte[] dataRcvBuf;

        public Connection(Socket s)
        {
            this.sock = s;
            this.dataRcvBuf = new byte[16];

            // Start listening for incoming data.  (If you want a multi-
            // threaded service, you can start this method up in a separate
            // thread.)
            this.BeginReceive();
        }

        // Call this method to set this connection's socket up to receive data.
        private void BeginReceive()
        {
            this.sock.BeginReceive(
                    this.dataRcvBuf, 0,
                    this.dataRcvBuf.Length,
                    SocketFlags.None,
                    new AsyncCallback(this.OnBytesReceived),
                    this);
        }

        // This is the method that is called whenever the socket receives
        // incoming bytes.
        protected void OnBytesReceived(IAsyncResult result)
        {
            // End the data receiving that the socket has done and get
            // the number of bytes read.
            int nBytesRec = this.sock.EndReceive(result);
            // If no bytes were received, the connection is closed (at
            // least as far as we're concerned).
            if (nBytesRec <= 0)
            {
                this.sock.Close();
                return;
            }
            // Convert the data we have to a string.
            string strReceived = this.encoding.GetString(
                this.dataRcvBuf, 0, nBytesRec);

            // ...Now, do whatever works best with the string data.
            // You could, for example, look at each character in the string
            // one-at-a-time and check for characters like the "end of text"
            // character ('\u0003') from a client indicating that they've finished
            // sending the current message.  It's totally up to you how you want
            // the protocol to work.

            // Whenever you decide the connection should be closed, call 
            // sock.Close() and don't call sock.BeginReceive() again.  But as long 
            // as you want to keep processing incoming data...

            // Set up again to get the next chunk of data.
            this.sock.BeginReceive(
                this.dataRcvBuf, 0,
                this.dataRcvBuf.Length,
                SocketFlags.None,
                new AsyncCallback(this.OnBytesReceived),
                this);

        }
    }
}

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using RabbitMQ.Client.MessagePatterns;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Telerik.OpenAccess.Cluster;

namespace CustomSecondLevelCacheClusterTransport
{

    public class RabbitMQCacheClusterTransport : SecondLevelCacheClusterTransportBase
    {
        public override int MaxMessageSize => -1;

        private string _localRoutingKey => string.Join(string.Empty, localIdentifier.Select(_ => _.ToString("X2")));

        private IConnection _rabbitMQConnection;
        private IModel _rabbitMQModel;
        private ISubscription _rabbitMQSubscription;
        private string _rabbitMQQueueName;
        private bool _isConsuming;

        //Timeout for consumer reconnection
        private static int millisecondsTimeout = 30000;
        private delegate void RabbitMQConsumerDelegate(int milisecondsTimeout);

        public override void Init(OpenAccessClusterMsgHandler msgHandler, string serverName, string identifier, IOpenAccessClusterTransportLog log)
        {
            this.handler = msgHandler;
            this.log = log;

            if (!ConnectToRabbitMQ())
                throw new Exception("Failed to connect to RabbitMQ");

            StartConsumingFromRabbitMQ(millisecondsTimeout);
        }

        public override void SendMessage(byte[] buffer)
        {
            try
            {
                IBasicProperties basicProperties = _rabbitMQModel.CreateBasicProperties();
                basicProperties.Persistent = true;

                _rabbitMQModel.BasicPublish(this.Localpath, _localRoutingKey, basicProperties, buffer);
            }
            catch (Exception e)
            {
                log.LogWarning("Publisher got exception {0}", e.Message);
            }
        }

        public override void Close()
        {
            lock (this)
            {
                closed = true;

                DisconnectFromRabbitMQ();
            }
            handler = null;
        }

        #region IDisposable Members

        protected override void Dispose(bool disposing)
        {
            if (closed == false && disposing)
            {
                Close();

                DisconnectFromRabbitMQ();
            }
        }

        #endregion

        #region RabbitMQ Communication Methods
        private bool ConnectToRabbitMQ()
        {
            try
            {
                DisconnectFromRabbitMQ();

                string hostName = string.IsNullOrEmpty(this.multicastAddress) ? "localhost" : this.multicastAddress;
                string virtualHost = ConnectionFactory.DefaultVHost;
                if (hostName.Contains("/"))
                {
                    virtualHost = hostName.Substring(hostName.IndexOf("/"));
                    hostName = hostName.Substring(0, hostName.IndexOf("/"));
                }
                var connectionFactory = new ConnectionFactory();
                connectionFactory.HostName = hostName;
                connectionFactory.VirtualHost = virtualHost;
                connectionFactory.Port = this.multicastPort == 0 ? AmqpTcpEndpoint.UseDefaultPort : this.multicastPort;
                connectionFactory.UserName = string.IsNullOrEmpty(this.multicastUser) ? ConnectionFactory.DefaultUser : this.multicastUser;
                connectionFactory.Password = string.IsNullOrEmpty(this.multicastPassword) ? ConnectionFactory.DefaultPass : this.multicastPassword;

                _rabbitMQConnection = connectionFactory.CreateConnection();
                _rabbitMQModel = _rabbitMQConnection.CreateModel();

                bool durable = true;
                if (string.IsNullOrEmpty(this.Localpath))
                    this.Localpath = "TelerikL2Cache";

                _rabbitMQModel.ExchangeDeclare(this.Localpath, "fanout", durable);


                _rabbitMQModel.BasicQos(0, 10, false);

                _rabbitMQQueueName = _rabbitMQModel.QueueDeclare();
                _rabbitMQModel.QueueBind(_rabbitMQQueueName, this.Localpath, _localRoutingKey);

                return true;
            }
            catch (BrokerUnreachableException e)
            {
                return false;
            }
        }

        private void StartConsumingFromRabbitMQ(int millisecondsTimeout)
        {
            _isConsuming = true;

            new RabbitMQConsumerDelegate(ConsumeMessages).BeginInvoke(millisecondsTimeout, null, null);
        }

        private void ConsumeMessages(int millisecondsTimeout)
        {
            bool autoAck = false;

            //create a subscription
            _rabbitMQSubscription = new Subscription(_rabbitMQModel, _rabbitMQQueueName, autoAck);

            while (_isConsuming)
            {
                try
                {
                    BasicDeliverEventArgs e = null;
                    if (_rabbitMQSubscription.Next(millisecondsTimeout, out e))
                    {
                        byte[] body = e.Body;
                        var bodyLength = body.Length;

                        if (!_localRoutingKey.Equals(e.RoutingKey))
                        {
                            handler.HandleMessage(new MemoryStream(body, 0, bodyLength, false));
                            log.LogInformation("Received {0} bytes", bodyLength);
                        }
                        else
                        {
                            log.LogInformation("Got my own eviction");
                        }
                        _rabbitMQSubscription.Ack(e);
                    }
                    else
                    {
                        throw new TimeoutException("Receiver timed out");
                    }
                }
                catch (Exception e)
                {
                    log.LogWarning("Receiver got exception {0}", e.Message);

                    //Renews the subscription
                    if (ConnectToRabbitMQ())
                        StartConsumingFromRabbitMQ(millisecondsTimeout);

                    break;
                }
            }
        }

        private void DisconnectFromRabbitMQ()
        {
            _isConsuming = false;
            try
            {
                if (_rabbitMQSubscription != null)
                    _rabbitMQSubscription.Close();
            }
            catch { }

            try
            {
                if (_rabbitMQConnection != null)
                    _rabbitMQConnection.Close();
            }
            catch { }

            try
            {
                if (_rabbitMQModel != null)
                    _rabbitMQModel.Abort();
            }
            catch { }

            _rabbitMQQueueName = null;
        }
        #endregion
    }
}

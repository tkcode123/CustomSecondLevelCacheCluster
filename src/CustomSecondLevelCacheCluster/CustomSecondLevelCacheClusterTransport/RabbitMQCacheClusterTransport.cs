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

        private delegate void RabbitMQConsumerDelegate();

        public override void Init(OpenAccessClusterMsgHandler msgHandler, string serverName, string identifier, IOpenAccessClusterTransportLog log)
        {
            this.handler = msgHandler;
            this.log = log;

            if (!ConnectToRabbitMQ())
                throw new Exception("Failed to connect to RabbitMQ");

            StartConsumingFromRabbitMQ();
        }

        public override void SendMessage(byte[] buffer)
        {
            IBasicProperties basicProperties = _rabbitMQModel.CreateBasicProperties();
            basicProperties.Persistent = true;

            _rabbitMQModel.BasicPublish(this.Localpath, _localRoutingKey, basicProperties, buffer);
        }

        public override void Close()
        {
            lock (this)
            {
                closed = true;

                _isConsuming = false;

                _rabbitMQConnection?.Close();
                _rabbitMQModel?.Abort();
            }
            handler = null;
        }

        #region IDisposable Members

        protected override void Dispose(bool disposing)
        {
            if (closed == false && disposing)
            {
                Close();

                _rabbitMQConnection?.Dispose();
                _rabbitMQModel?.Dispose();
            }
        }

        #endregion

        #region RabbitMQ Communication Methods
        private bool ConnectToRabbitMQ()
        {
            try
            {
                string hostName = string.IsNullOrEmpty(this.multicastAddress) ? "localhost" : this.multicastAddress;
                string virtualHost = ConnectionFactory.DefaultVHost;
                if(hostName.Contains("/"))
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

                return true;
            }
            catch (BrokerUnreachableException e)
            {
                return false;
            }
        }

        private void StartConsumingFromRabbitMQ()
        {
            _rabbitMQModel.BasicQos(0, 1, false);

            _rabbitMQQueueName = _rabbitMQModel.QueueDeclare();
            _rabbitMQModel.QueueBind(_rabbitMQQueueName, this.Localpath, _localRoutingKey);
            _isConsuming = true;

            new RabbitMQConsumerDelegate(ConsumeMessages).BeginInvoke(null, null);
        }

        private void ConsumeMessages()
        {
            bool autoAck = false;

            //create a subscription
            _rabbitMQSubscription = new Subscription(_rabbitMQModel, _rabbitMQQueueName, autoAck);

            while (_isConsuming)
            {
                BasicDeliverEventArgs e = _rabbitMQSubscription.Next();
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
        }
        #endregion
    }
}

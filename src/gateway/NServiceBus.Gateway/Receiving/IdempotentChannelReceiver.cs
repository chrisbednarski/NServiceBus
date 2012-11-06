﻿namespace NServiceBus.Gateway.Receiving
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Transactions;
    using Channels;
    using Channels.Http;
    using DataBus;
    using Deduplication;
    using HeaderManagement;
    using log4net;
    using Notifications;
    using Sending;
    using Unicast.Transport;
    using Utils;

    public class IdempotentChannelReceiver : IReceiveMessagesFromSites
    {
        public IdempotentChannelReceiver(IChannelFactory channelFactory, IDeduplicateMessages deduplicator)
        {
            this.channelFactory = channelFactory;
            this.deduplicator = deduplicator;
        }

        public event EventHandler<MessageReceivedOnChannelArgs> MessageReceived;

        public IDataBus DataBus { get; set; }

        public void Start(Channel channel, int numWorkerThreads)
        {
            channelReceiver = channelFactory.GetReceiver(channel.Type);
            channelReceiver.DataReceived += DataReceivedOnChannel;
            channelReceiver.Start(channel.Address,numWorkerThreads);
        }

        void DataReceivedOnChannel(object sender, DataReceivedOnChannelArgs e)
        {
            using (e.Data)
            {
                var callInfo = GetCallInfo(e);

                Logger.DebugFormat("Received message of type {0} for client id: {1}", callInfo.Type, callInfo.ClientId);

                using (var scope = DefaultTransactionScope())
                {
                    switch (callInfo.Type)
                    {
                        case CallType.DatabusProperty: HandleDatabusProperty(callInfo); break;
                        case CallType.Submit: HandleSubmit(callInfo); break;
                    }
                    scope.Complete();
                }
            }
        }

        static TransactionScope DefaultTransactionScope()
        {
            return new TransactionScope(TransactionScopeOption.RequiresNew,
                                        new TransactionOptions
                                            {
                                                IsolationLevel = IsolationLevel.ReadCommitted,
                                                Timeout = TimeSpan.FromSeconds(30)
                                            });
        }

        CallInfo GetCallInfo(DataReceivedOnChannelArgs receivedData)
        {
            var headers = receivedData.Headers;
       
            string callType = headers[GatewayHeaders.CallTypeHeader];
            if (!Enum.IsDefined(typeof(CallType), callType))
                throw new ChannelException(400, "Required header '" + GatewayHeaders.CallTypeHeader + "' missing.");

            var type = (CallType)Enum.Parse(typeof(CallType), callType);

            var clientId = headers[GatewayHeaders.ClientIdHeader];
            if (clientId == null)
                throw new ChannelException(400, "Required header '" + GatewayHeaders.ClientIdHeader + "' missing.");

            return new CallInfo
            {
                ClientId = clientId,
                Type = type,
                Headers = headers,
                Data = receivedData.Data,
            };
        }

        void HandleSubmit(CallInfo callInfo)
        {
            using (var stream = new MemoryStream())
            {
                callInfo.Data.CopyTo_net35(stream);
                stream.Position = 0;

                CheckHashOfGatewayStream(stream, callInfo.Headers[HttpHeaders.ContentMd5Key]);

                IDictionary<string, string> databusHeaders;
                if (!deduplicator.DeduplicateMessage(callInfo.ClientId, DateTime.UtcNow, out databusHeaders))
                {
                    Logger.InfoFormat("Message with id: {0} is already acked, dropping the request", callInfo.ClientId);
                    return;
                }

                var msg = new TransportMessage
                {
                    Body = new byte[stream.Length],
                    Headers = new Dictionary<string, string>(),
                    MessageIntent = MessageIntentEnum.Send,
                    Recoverable = true
                };

                stream.Read(msg.Body, 0, msg.Body.Length);
                databusHeaders.ToList().ForEach(kv => callInfo.Headers[kv.Key] = kv.Value);

                if (callInfo.Headers.ContainsKey(GatewayHeaders.IsGatewayMessage))
                    HeaderMapper.Map(callInfo.Headers, msg);

                MessageReceived(this, new MessageReceivedOnChannelArgs { Message = msg });
            }
        }

        void HandleDatabusProperty(CallInfo callInfo)
        {
            if (DataBus == null)
                throw new InvalidOperationException("Databus transmission received without a databus configured");

            TimeSpan timeToBeReceived;
            if (!TimeSpan.TryParse(callInfo.Headers["NServiceBus.TimeToBeReceived"], out timeToBeReceived))
                timeToBeReceived = TimeSpan.FromHours(1);

            string newDatabusKey = DataBus.Put(callInfo.Data, timeToBeReceived);
            using(var databusStream = DataBus.Get(newDatabusKey))
                CheckHashOfGatewayStream(databusStream, callInfo.Headers[HttpHeaders.ContentMd5Key]);

            var specificDataBusHeaderToUpdate = callInfo.Headers[GatewayHeaders.DatabusKey];
            deduplicator.InsertDataBusProperty(callInfo.ClientId, specificDataBusHeaderToUpdate, newDatabusKey);
        }

        void CheckHashOfGatewayStream(Stream input, string md5Hash)
        {
            if (md5Hash == null)
                throw new ChannelException(400, "Required header '" + HttpHeaders.ContentMd5Key + "' missing.");

            if (md5Hash != Hasher.Hash(input))
                throw new ChannelException(412, "MD5 hash received does not match hash calculated on server. Please resubmit.");
        }

        public void Dispose()
        {
            channelReceiver.DataReceived -= DataReceivedOnChannel;
            channelReceiver.Dispose();
        }

        IChannelReceiver channelReceiver;
        readonly IChannelFactory channelFactory;
        readonly IDeduplicateMessages deduplicator;

        static readonly ILog Logger = LogManager.GetLogger("NServiceBus.Gateway");

    }
}

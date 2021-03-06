namespace NServiceBus.Unicast.Messages
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Linq;
    using MessageInterfaces;
    using Pipeline;


    [Obsolete("This is a prototype API. May change in minor version releases.")]
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class LogicalMessageFactory
    {
        public MessageMetadataRegistry MessageMetadataRegistry { get; set; }
        
        public IMessageMapper MessageMapper { get; set; }

        public PipelineExecutor PipelineExecutor { get; set; }

        public LogicalMessage Create(object message)
        {
            return Create(message.GetType(), message);
        }

        public LogicalMessage Create(Type messageType, object message)
        {
             var headers = GetMessageHeaders(message);

            return Create(messageType, message, headers);
        }

        public LogicalMessage Create(Type messageType, object message, Dictionary<string, string> headers)
        {
            var realMessageType = MessageMapper.GetMappedTypeFor(messageType);

            return new LogicalMessage(MessageMetadataRegistry.GetMessageDefinition(realMessageType), message, headers, this);
        }

        //in v5 we can skip this since we'll only support one message and the creation of messages happens under our control so we can capture 
        // the real message type without using the mapper
        [ObsoleteEx(RemoveInVersion = "5.0")]
        public List<LogicalMessage> CreateMultiple(params object[] messages)
        {
            return CreateMultiple((IEnumerable<object>)messages);
        }

        [ObsoleteEx(RemoveInVersion = "5.0")]
        public List<LogicalMessage> CreateMultiple(IEnumerable<object> messages)
        {
            if (messages == null)
            {
                return new List<LogicalMessage>();
            }

            return messages.Select(m =>
            {
                var messageType = MessageMapper.GetMappedTypeFor(m.GetType());
                var headers = GetMessageHeaders(m);

                return new LogicalMessage(MessageMetadataRegistry.GetMessageDefinition(messageType), m, headers, this);
            }).ToList();
        }

        Dictionary<string, string> GetMessageHeaders(object message)
        {
            Dictionary<object, Dictionary<string, string>> outgoingHeaders;

            if (!PipelineExecutor.CurrentContext.TryGet("NServiceBus.OutgoingHeaders", out outgoingHeaders))
            {
                return new Dictionary<string, string>();
            }
            Dictionary<string, string> outgoingHeadersForThisMessage;

            if (!outgoingHeaders.TryGetValue(message, out outgoingHeadersForThisMessage))
            {
                return new Dictionary<string, string>();
            }

            //remove the entry to allow memory to be reclaimed
            outgoingHeaders.Remove(message);

            return outgoingHeadersForThisMessage;
        }
    }
}
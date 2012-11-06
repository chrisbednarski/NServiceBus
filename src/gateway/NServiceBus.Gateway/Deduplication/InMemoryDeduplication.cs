namespace NServiceBus.Gateway.Deduplication
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public class InMemoryDeduplication : IDeduplicateMessages
    {
        public void InsertDataBusProperty(string clientId, string key, string value)
        {
            lock (persistence)
            {
                var item = persistence.SingleOrDefault(m => m.Id == clientId);
                if (item == null)
                    item = new MessageData(clientId);

                item.DataBus[key] = value;
                persistence.Add(item);
            }
        }

        public bool DeduplicateMessage(string clientId, DateTime timeReceived, out IDictionary<string, string> databusProperties)
        {
            databusProperties = null;
            lock (persistence)
            {
                var item = persistence.SingleOrDefault(m => m.Id == clientId);
                if (item == null)
                    item = new MessageData(clientId);
                else if (item.Deduplicated)
                    return false;

                item.Received = timeReceived;
                item.Deduplicated = true;

                databusProperties = item.DataBus;
            }
            return true;
        }

        private class MessageData
        {
            public MessageData(string id) { Id = id; }

            public string Id;
            public readonly IDictionary<string, string> DataBus = new Dictionary<string, string>();
            public bool Deduplicated;
            public DateTime Received;
        }

        readonly ISet<MessageData> persistence = new HashSet<MessageData>();
    }
}

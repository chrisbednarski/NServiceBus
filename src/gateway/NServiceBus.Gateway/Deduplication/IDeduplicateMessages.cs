namespace NServiceBus.Gateway.Deduplication
{
    using System;
    using System.Collections.Generic;

    public interface IDeduplicateMessages
    {
        void InsertDataBusProperty(string clientId, string key, string value);
        bool DeduplicateMessage(string clientId, DateTime timeReceived, out IDictionary<string, string> databusProperties);
    }
}

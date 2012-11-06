namespace NServiceBus.Gateway.Deduplication
{
    using System.Collections.Generic;

    public interface IReassembleGatewayHeaders
    {
        void InsertHeader(string clientId, string headerKey, string headerValue);
        IDictionary<string, string> Reassemble(string clientId, IDictionary<string, string> input);
    }
}

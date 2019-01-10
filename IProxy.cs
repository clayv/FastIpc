using System;

namespace CVV
{
    interface IProxy
    {
        /// <summary>Nongeneric version of the LocalInstance </summary>
        object LocalInstanceUntyped { get; }
        FastIpc Channel { get; }
        int? ObjectID { get; }
        byte DomainAddress { get; }
        void Disconnect();
        Type ObjectType { get; }
        bool IsDisconnected { get; }

        /// <summary>Connects the proxy for channel implementors. Used by FastChannel.</summary>
        void RegisterLocal(FastIpc fastChannel, int? objectID, Action onDisconnect);
    }
}

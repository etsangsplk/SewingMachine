using System;
using System.Fabric;
using Microsoft.ServiceFabric.Services.Runtime;

namespace SewingMachine
{
    /// <summary>
    /// Represents base class for stateful service based on the <see cref="KeyValueStoreReplica"/>, an associative replicated key-value store.
    /// </summary>
    public abstract class KeyValueStatefulService: StatefulServiceBase
    {
        readonly Lazy<KeyValueStoreReplica> store;

        protected KeyValueStatefulService(StatefulServiceContext serviceContext)
            : this(serviceContext, new KeyValueStateProvider())
        {
        }

        protected KeyValueStatefulService(StatefulServiceContext serviceContext, KeyValueStateProvider stateProviderReplica)
            : base(serviceContext, stateProviderReplica)
        {
            store = new Lazy<KeyValueStoreReplica>(()=>stateProviderReplica.StoreReplica);
        }

        protected SewingSession OpenSession()
        {
            return new SewingSession(store.Value);
        }
    }
}
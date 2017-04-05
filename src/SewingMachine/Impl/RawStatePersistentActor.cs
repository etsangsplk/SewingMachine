using System;
using System.Fabric;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Actors.Runtime;

namespace SewingMachine.Impl
{
    /// <summary>
    /// The base class for actors accessing its state directly via <see cref="RawStore"/> instead of using <see cref="IActorStateManager"/>.
    /// </summary>
    abstract class RawStatePersistentActor : Actor
    {
        static readonly FieldInfo ReplicaField;

        protected RawAccessorToKeyValueStoreReplica RawStore { get; private set; }

        static RawStatePersistentActor()
        {
            ReplicaField = typeof(KvsActorStateProvider).GetFields(BindingFlags.Public | BindingFlags.NonPublic |
                                                                    BindingFlags.Instance)
                .Single(
                    fi =>
                        fi.FieldType == typeof(KeyValueStoreReplica) ||
                        fi.FieldType.IsSubclassOf(typeof(KeyValueStoreReplica)));
        }


        protected RawStatePersistentActor(ActorService actorService, ActorId actorId)
            : base(actorService, actorId)
        {
        }

        void SetReplica(KeyValueStoreReplica replica)
        {
            RawStore = new RawAccessorToKeyValueStoreReplica(replica);
        }

        public static Task RegisterActorAsync<TActor>(TimeSpan timeout = default(TimeSpan), CancellationToken cancellationToken = default(CancellationToken))
            where TActor : RawStatePersistentActor
        {
            return ActorRuntime.RegisterActorAsync<TActor>((context, actorTypeInfo) =>
            {
                return new ActorService(context, actorTypeInfo, null, (actor, provider) =>
                {
                    var persistent = provider as KvsActorStateProvider;
                    if (persistent != null)
                    {
                        var replica = (KeyValueStoreReplica)ReplicaField.GetValue(provider);
                        ((RawStatePersistentActor)actor).SetReplica(replica);

                        return new ThrowingActorStateManager();
                    }

                    throw new ArgumentException("Event sourced actor must be persistent and use KvsActorStateProvider");
                });

            }, timeout, cancellationToken);
        }
    }
}
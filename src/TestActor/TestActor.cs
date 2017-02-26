using System;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Actors.Runtime;
using NUnit.Framework;
using SewingMachine;
using TestActor.Interfaces;

namespace TestActor
{
    [StatePersistence(StatePersistence.Persisted)]
    class TestActor : RawStatePersistentActor, ITestActor
    {
        static readonly Task<int> Completed = Task.FromResult(0);
        static unsafe char* A1 = (char*)Marshal.StringToHGlobalUni("A1");
        static unsafe char* A2 = (char*)Marshal.StringToHGlobalUni("A2");
        static unsafe char* B = (char*)Marshal.StringToHGlobalUni("B");

        const string V = "Value";
        static unsafe char* Value = (char*)Marshal.StringToHGlobalUni(V);
        static unsafe ReplicaKeyValue A1_Value = new ReplicaKeyValue(A1, (byte*)Value, (V.Length + 1) * 2);
        static unsafe ReplicaKeyValue A2_Value = new ReplicaKeyValue(A2, (byte*)Value, (V.Length + 1) * 2);
        static unsafe ReplicaKeyValue B_Value = new ReplicaKeyValue(A2, (byte*)Value, (V.Length + 1) * 2);


        public TestActor(ActorService actorService, ActorId actorId)
            : base(actorService, actorId)
        { }

        protected override Task OnActivateAsync()
        {
            return Completed;
        }

        public Task When_accessing_StateManager_should_throw(CancellationToken cancellationToken)
        {
            Assert.ThrowsAsync<InvalidOperationException>(
                () => StateManager.GetStateAsync<int>("count", cancellationToken));

            return Completed;
        }

        public async Task When_value_added_should_get_it(CancellationToken cancellationToken)
        {
            using (var tx = RawStore.BeginTransaction())
            {
                RawStore.Add(tx, A1_Value);
                await tx.CommitAsync().ConfigureAwait(false);
            }

            using (var tx = RawStore.BeginTransaction())
            {
                unsafe
                {
                    var item = (Item)RawStore.TryGet(tx, A1, Map);

                    Assert.NotNull(item);

                    Assert.AreEqual("Value", item.Value);
                    Assert.AreEqual("A1", item.Key);
                }
            }
        }

        static unsafe object Map(RawAccessorToKeyValueStoreReplica.RawItem arg)
        {
            var value = new string((char*)arg.Value, 0, arg.ValueLength / 2 - 1);
            var key = new string(arg.Key);

            return new Item
            {
                Key = key,
                Value = value,
                SequenceNumber = arg.SequenceNumber
            };
        }

        class Item
        {
            public string Value { get; set; }
            public string Key { get; set; }
            public long SequenceNumber { get; set; }
        }
    }
}

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
        static long _callCounter;
        static readonly Task<int> Completed = Task.FromResult(0);
        string _key;

        public TestActor(ActorService actorService, ActorId actorId)
            : base(actorService, actorId)
        { }

        protected override Task OnActivateAsync()
        {
            return Completed;
        }

        protected override Task OnPreActorMethodAsync(ActorMethodContext actorMethodContext)
        {
            _key = Interlocked.Increment(ref _callCounter).ToString();
            return base.OnPreActorMethodAsync(actorMethodContext);
        }

        public Task When_accessing_StateManager_should_throw(CancellationToken cancellationToken)
        {
            Assert.ThrowsAsync<InvalidOperationException>(
                () => StateManager.GetStateAsync<int>("count", cancellationToken));

            return Completed;
        }

        public async Task When_value_Added_should_Get_it(CancellationToken cancellationToken)
        {
            const string expected = "Value";
            var unsafeKey = await Add(_key, expected);

            AssertGet(unsafeKey, expected, _key);
        }

        public Task When_TryRemove_non_existent_value_should_not_fail(CancellationToken cancellationToken)
        {
            IntPtr unsafeKey;
            Create(_key, "Value", out unsafeKey);

            using (var tx = RawStore.BeginTransaction())
            {
                unsafe
                {
                    Assert.False(RawStore.TryRemove(tx, (char*)unsafeKey, 0), "Should not remove non-existing entry");
                }
            }

            return Completed;
        }

        public Task When_Remove_non_existent_value_should_throw(CancellationToken cancellationToken)
        {
            IntPtr unsafeKey;
            Create(_key, "Value", out unsafeKey);

            using (var tx = RawStore.BeginTransaction())
            {
                unsafe
                {
                    Assert.Throws(Is.AssignableTo<Exception>(), () => RawStore.Remove(tx, (char*)unsafeKey, 0), "Should not remove non-existing entry");
                }
            }

            return Completed;
        }

        async Task<IntPtr> Add(string key, string expected)
        {
            IntPtr unsafeKey;
            using (var tx = RawStore.BeginTransaction())
            {
                RawStore.Add(tx, Create(key, expected, out unsafeKey));
                await tx.CommitAsync().ConfigureAwait(false);
            }
            return unsafeKey;
        }

        unsafe void AssertGet(IntPtr unsafeKey, string expected, string key)
        {
            using (var tx = RawStore.BeginTransaction())
            {
                var item = (Item) RawStore.TryGet(tx, (char*) unsafeKey, Map);

                Assert.NotNull(item);

                Assert.AreEqual(expected, item.Value);
                Assert.AreEqual(key, item.Key);
            }
        }

        static unsafe ReplicaKeyValue Create(string key, string value, out IntPtr unsafeKey)
        {
            unsafeKey = Marshal.StringToHGlobalUni(key);
            var k = (char*)unsafeKey;
            var v = (char*)Marshal.StringToHGlobalUni(value);
            return new ReplicaKeyValue(k, (byte*)v, (value.Length + 1) * 2);
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

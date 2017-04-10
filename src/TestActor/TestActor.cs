using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Actors.Runtime;
using NUnit.Framework;
using SewingMachine;
using SewingMachine.Impl;
using TestActor.Interfaces;

namespace TestActor
{
    [StatePersistence(StatePersistence.Persisted)]
    class TestActor : RawStatePersistentActor, ITestActor
    {
        static long _callCounter;
        static readonly Task<int> Completed = Task.FromResult(0);
        string key;

        public TestActor(ActorService actorService, ActorId actorId)
            : base(actorService, actorId)
        { }

        protected override Task OnActivateAsync()
        {
            return Completed;
        }

        protected override Task OnPreActorMethodAsync(ActorMethodContext actorMethodContext)
        {
            key = Interlocked.Increment(ref _callCounter).ToString();
            return base.OnPreActorMethodAsync(actorMethodContext);
        }

        public Task When_accessing_StateManager_should_throw(CancellationToken cancellationToken)
        {
            Assert.ThrowsAsync<InvalidOperationException>(
                () => StateManager.GetStateAsync<int>("count", cancellationToken));

            return Completed;
        }

        public async Task When_Add_should_Get_value(CancellationToken cancellationToken)
        {
            const string expected = "Value";
            var unsafeKey = await Add(key, expected);

            AssertGet(unsafeKey, expected, key);
        }

        public async Task When_TryAdd_should_fail_on_existing_key(CancellationToken cancellationToken)
        {
            const string expected = "Value";
            await Add(key, expected);

            using (var tx = OpenSession())
            {
                IntPtr unsafeKey;
                Assert.False(tx.TryAdd(Create(key, expected, out unsafeKey)));
            }
        }

        public Task When_TryRemove_non_existent_value_should_not_fail(CancellationToken cancellationToken)
        {
            IntPtr unsafeKey;
            Create(key, "Value", out unsafeKey);

            using (var tx = OpenSession())
            {
                Assert.False(tx.TryRemove(unsafeKey, 0), "Should not remove non-existing entry");
            }

            return Completed;
        }

        public Task When_Remove_non_existent_value_should_throw(CancellationToken cancellationToken)
        {
            IntPtr unsafeKey;
            Create(key, "Value", out unsafeKey);

            using (var tx = OpenSession())
            {
                Assert.Throws(Is.AssignableTo<Exception>(), () => tx.Remove(unsafeKey, 0), "Should not remove non-existing entry");
            }

            return Completed;
        }

        public async Task When_Update_existent_value_should_replace(CancellationToken cancellationToken)
        {
            var unsafeKey = await Add(key, "Value");

            using (var tx = OpenSession())
            {
                var item = (Item)tx.TryGet(unsafeKey, Map);
                tx.Update(Create(key, "V", out unsafeKey), item.SequenceNumber);

                await tx.SaveChangesAsync();
            }

            AssertGet(unsafeKey, "V", key);
        }

        public async Task When_TryUpdate_existent_value_with_wrong_version_should_throw(CancellationToken cancellationToken)
        {
            var unsafeKey = await Add(key, "Value");

            using (var tx = OpenSession())
            {
                var item = (Item)tx.TryGet(unsafeKey, Map);
                Assert.Throws<COMException>(() => tx.TryUpdate(Create(key, "V", out unsafeKey), item.SequenceNumber + 1));

                await tx.SaveChangesAsync();
            }

            AssertGet(unsafeKey, "Value", key);
        }

        public async Task When_Enumerate_should_go_through_all_prefixed_value(CancellationToken cancellationToken)
        {
            const string expected = "Value";
            var unsafeKey = await Add(key, expected);
            await Add(key + "1", expected);
            await Add(key + "2", expected);

            using (var tx = OpenSession())
            {
                var results = new List<object>();
                tx.Enumerate(unsafeKey, Map, o => results.Add(o));

                var array = results.Cast<Item>().OrderBy(i => i.Key).ToArray();

                Assert.AreEqual(3, array.Length);

                Assert.AreEqual(key, array[0].Key);
                Assert.AreEqual(expected, array[0].Value);

                Assert.AreEqual(key + "1", array[1].Key);
                Assert.AreEqual(expected, array[1].Value);

                Assert.AreEqual(key + "2", array[2].Key);
                Assert.AreEqual(expected, array[2].Value);
            }
        }

        public async Task When_registering_BeforeSafe_should_execute_it_when_saving(CancellationToken cancellationToken)
        {
            using (var tx = OpenSession())
            {
                var counter = 0;
                tx.BeforeSave(_ => { counter++; });

                await tx.SaveChangesAsync().ConfigureAwait(false);
                Assert.AreEqual(1, counter);
            }
        }

        async Task<IntPtr> Add(string key, string expected)
        {
            IntPtr unsafeKey;
            using (var tx = OpenSession())
            {
                tx.Add(Create(key, expected, out unsafeKey));
                await tx.SaveChangesAsync().ConfigureAwait(false);
            }
            return unsafeKey;
        }

        void AssertGet(IntPtr unsafeKey, string expected, string key)
        {
            using (var tx = OpenSession())
            {
                var item = (Item)tx.TryGet(unsafeKey, Map);

                Assert.NotNull(item);

                Assert.AreEqual(expected, item.Value);
                Assert.AreEqual(key, item.Key);
            }
        }

        static ReplicaKeyValue Create(string key, string value, out IntPtr unsafeKey)
        {
            unsafeKey = Marshal.StringToHGlobalUni(key);
            var k = unsafeKey;
            var v = Marshal.StringToHGlobalUni(value);
            return new ReplicaKeyValue(k, v, (value.Length + 1) * 2);
        }

        static unsafe object Map(RawItem arg)
        {
            var value = new string((char*)arg.Value, 0, arg.ValueLength / 2 - 1);
            var key = new string((char*)arg.Key);

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

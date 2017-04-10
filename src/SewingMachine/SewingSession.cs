using System;
using System.Collections.Generic;
using System.Fabric;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using SewingMachine.Impl;

namespace SewingMachine
{
    /// <summary>
    ///     The raw/unsafe accessor to the underlying ServiceFabric storage behind <see cref="KeyValueStoreReplica" />.
    /// </summary>
    /// <remarks>
    ///     This class unleashes the underlying ServiceFabric storage behind <see cref="KeyValueStoreReplica" />.
    ///     Unfortunately, the Interop types provided in
    ///     <see cref="System.Fabric" /> are internal, which requires all the IL emit heavy lifting done in the
    ///     <see cref="Invoker" /> class. If these types were public,
    ///     no IL emit would be required.
    /// </remarks>
    public class SewingSession : IDisposable
    {
        const sbyte True = 1;
        static readonly Invoker Helper = new Invoker();

        readonly KeyValueStoreReplica store;
        Transaction tx;
        List<Action<SewingSession>> beforeSave;

        public SewingSession(KeyValueStoreReplica store)
        {
            this.store = store;
            tx = store.CreateTransaction();
        }

        public void BeforeSave(Action<SewingSession> onSave)
        {
            if (beforeSave == null)
            {
                beforeSave = new List<Action<SewingSession>>();
            }

            beforeSave.Add(onSave);
        }

        public async Task<long> SaveChangesAsync()
        {
            if (beforeSave != null)
            {
                foreach (var action in beforeSave)
                {
                    action(this);
                }
                beforeSave = null;
            }

            var position = await tx.CommitAsync();
            Dispose();
            return position;
        }

        public void Dispose()
        {
            tx?.Dispose();
            tx = null;
        }

        public void Add(ReplicaKeyValue kv)
        {
            Helper.Add(store, tx, kv.Key, kv.ValueLength, kv.Value);
        }

        public bool TryAdd(ReplicaKeyValue kv)
        {
            return Helper.TryAdd(store, tx, kv.Key, kv.ValueLength, kv.Value) == True;
        }

        public void Update(ReplicaKeyValue kv, long expectedVersion)
        {
            Helper.Update(store, tx, kv.Key, kv.ValueLength, kv.Value, expectedVersion);
        }

        public bool TryUpdate(ReplicaKeyValue kv, long expectedVersion)
        {
            return Helper.TryUpdate(store, tx, kv.Key, kv.ValueLength, kv.Value, expectedVersion) == True;
        }

        public void Remove(IntPtr key, long expectedVersion)
        {
            Helper.Remove(store, tx, key, expectedVersion);
        }

        public bool TryRemove(IntPtr key, long expectedVersion)
        {
            return Helper.TryRemove(store, tx, key, expectedVersion) == True;
        }

        public bool Contains(IntPtr key)
        {
            return Helper.Contains(store, tx, key) == True;
        }

        public object TryGet(IntPtr key, Func<RawItem, object> mapper)
        {
            return Helper.TryGet(store, tx, key, mapper);
        }

        public void Enumerate(IntPtr prefix, Func<RawItem, object> mapper, Action<object> onValue)
        {
            object enumerator = null;
            try
            {
                enumerator = Helper.EnumerateByKey2(store, tx, prefix, True);

                while (Helper.NativeEnumeratorTryMoveNext(enumerator) == True)
                {
                    var value = Helper.NativeEnumeratorGetCurrent(enumerator, mapper);
                    if (value != null)
                        onValue(value);
                }
            }
            finally
            {
                if (enumerator != null)
                    Marshal.FinalReleaseComObject(enumerator);
            }
        }

        public static void Enumerate(IEnumerator<KeyValueStoreNotification> onReplication, Func<RawItem, object> mapper,
            Action<object> onValue)
        {
            // System.Fabric.KeyValueStoreNotificationEnumerator
            // private NativeRuntime.IFabricKeyValueStoreNotificationEnumerator2 nativeEnumerator;
            var enumerator = Helper.GetNativeNotificationEnumerator2(onReplication);
            while (Helper.NativeNotificationEnumeratorTryMoveNext(enumerator) == True)
            {
                var value = Helper.NativeNotificationEnumeratorGetCurrent(enumerator, mapper);
                if (value != null)
                    onValue(value);
            }
        }
    }
}
using System;
using System.Fabric;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.InteropServices;

namespace SewingMachine
{
    /// <summary>
    /// The raw/unsafe accessor to the underlying ServiceFabric storage behind <see cref="KeyValueStoreReplica"/>.
    /// </summary>
    /// <remarks>
    /// This class unleashes the underlying ServiceFabric storage behind <see cref="KeyValueStoreReplica"/>. Unfortunately, the Interop types provided in
    /// <see cref="System.Fabric"/> are internal, which requires all the IL emit heavy lifting done in the <see cref="Invoker"/> class. If these types were public,
    /// no IL emit would be required.
    /// </remarks>
    public class RawAccessorToKeyValueStoreReplica
    {
        const sbyte True = 1;
        static readonly Invoker Helper = new Invoker();

        readonly KeyValueStoreReplica _store;

        public RawAccessorToKeyValueStoreReplica(KeyValueStoreReplica store)
        {
            _store = store;
        }

        public Transaction BeginTransaction()
        {
            return _store.CreateTransaction();
        }

        public unsafe void Add(TransactionBase tx, ReplicaKeyValue kv)
        {
            Helper.Add(_store, tx, new IntPtr(kv.Key), kv.Length, new IntPtr(kv.Value));
        }

        public unsafe bool TryAdd(TransactionBase tx, ReplicaKeyValue kv)
        {
            return Helper.TryAdd(_store, tx, new IntPtr(kv.Key), kv.Length, new IntPtr(kv.Value)) == True;
        }

        public unsafe void Update(TransactionBase tx, ReplicaKeyValue kv, long expectedVersion)
        {
            Helper.Update(_store, tx, new IntPtr(kv.Key), kv.Length, new IntPtr(kv.Value), expectedVersion);
        }

        public unsafe bool TryUpdate(TransactionBase tx, ReplicaKeyValue kv, long expectedVersion)
        {
            return Helper.TryUpdate(_store, tx, new IntPtr(kv.Key), kv.Length, new IntPtr(kv.Value), expectedVersion) == True;
        }

        public unsafe void Remove(TransactionBase tx, char* key, long expectedVersion)
        {
            Helper.Remove(_store, tx, new IntPtr(key), expectedVersion);
        }

        public unsafe bool TryRemove(TransactionBase tx, char* key, long expectedVersion)
        {
            return Helper.TryRemove(_store, tx, new IntPtr(key), expectedVersion) == True;
        }

        public unsafe bool Contains(TransactionBase tx, char* key)
        {
            return Helper.Contains(_store, tx, new IntPtr(key)) == True;
        }

        public unsafe object TryGet(TransactionBase tx, char* key, Func<RawItem, object> mapper)
        {
            return Helper.TryGet(_store, tx, new IntPtr(key), mapper);
        }

        public unsafe void Enumerate(TransactionBase tx, char* prefix, Func<RawItem, object> mapper, Action<object> onValue)
        {
            object enumerator = null;
            try
            {
                enumerator = Helper.EnumerateByKey2(_store, tx, new IntPtr(prefix), True);

                while (Helper.NativeEnumeratorTryMoveNext (enumerator) == True)
                {
                    var value = Helper.NativeEnumeratorGetCurrent(enumerator, mapper);
                    if (value != null)
                    {
                        onValue(value);
                    }
                }
            }
            finally
            {
                if (enumerator != null)
                {
                    Marshal.FinalReleaseComObject(enumerator);
                }
            }
        }

        class Invoker
        {
            // write
            public readonly Action<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr> Add;
            public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, sbyte> TryAdd;
            public readonly Action<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, long> Update;
            public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, long, sbyte> TryUpdate;
            public readonly Action<KeyValueStoreReplica, TransactionBase, IntPtr, long> Remove;
            public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, long, sbyte> TryRemove;
            public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, sbyte> Contains;
            public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, Func<RawItem, object>, object> TryGet;
            public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, sbyte, object> EnumerateByKey2;
            public readonly Func<object, sbyte> NativeEnumeratorTryMoveNext;
            public readonly Func<object, Func<RawItem, object>, object> NativeEnumeratorGetCurrent;

            public Invoker()
            {
                var methods = InternalFabric.NativeReplicaType.GetMethods().ToDictionary(mi => mi.Name);

                // Add
                // replica, tx, key, lenght, value
                Add = Build<Action<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr>>(methods["Add"], EmitNonreadingMethod);
                TryAdd = Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, sbyte>>(methods["TryAdd"], EmitNonreadingMethod);

                // Update
                // replica, tx, key, lenght, value, sequenceNumber
                Update = Build<Action<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, long>>(methods["Update"], EmitNonreadingMethod);
                TryUpdate = Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, long, sbyte>>(methods["TryUpdate"], EmitNonreadingMethod);

                // Remove
                // replica, tx, key, sequenceNumber
                Remove = Build<Action<KeyValueStoreReplica, TransactionBase, IntPtr, long>>(methods["Remove"], EmitNonreadingMethod);
                TryRemove = Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, long, sbyte>>(methods["TryRemove"], EmitNonreadingMethod);

                // Contains
                Contains = Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, sbyte>>(methods["Contains"], EmitNonreadingMethod);

                // TryGetValue
                var mapFromNativeToObject = BuildMappingFromNativeResultToObject();
                TryGet = BuildTryGet(methods["TryGet"], mapFromNativeToObject);

                // EnumerateByKey2
                EnumerateByKey2 = Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, sbyte, object>>(methods["EnumerateByKey2"], EmitNonreadingMethod);

                // NativeEnumeratorMoveNext
                var enumerator = InternalFabric.NativeRuntimeType.DeclaredNestedTypes.Single(t => t.Name == "IFabricKeyValueStoreItemEnumerator2");
                var enumeratorMethods = enumerator.GetMethods().ToDictionary(mi => mi.Name);
                NativeEnumeratorTryMoveNext = Build<Func<object, sbyte>>(enumeratorMethods["TryMoveNext"],
                    (mi, il, parameterTypes) =>
                    {
                        il.Emit(OpCodes.Ldarg_0);
                        il.Emit(OpCodes.Castclass, enumerator);
                        il.EmitCall(OpCodes.Callvirt, mi, null);
                        il.Emit(OpCodes.Ret);
                    });

                NativeEnumeratorGetCurrent = Build<Func<object, Func<RawItem, object>, object>>(enumeratorMethods["get_Current"],
                   (mi, il, parameterTypes) =>
                   {
                       il.Emit(OpCodes.Ldarg_0);
                       il.Emit(OpCodes.Castclass, enumerator);
                       il.EmitCall(OpCodes.Callvirt, mi, null); // stack: IFabricKeyValueStoreItemResult
                       il.Emit(OpCodes.Ldarg_1); // stack: IFabricKeyValueStoreItemResult, mapper

                       il.EmitCall(OpCodes.Call, mapFromNativeToObject, null); // stack: object

                       il.Emit(OpCodes.Ret);
                   });
            }

            static TDelegate Build<TDelegate>(MethodInfo nativeMethod, Action<MethodInfo, ILGenerator, Type[]> emitBody)
            {
                // first three param types are always
                //typeof(KeyValueStoreReplica), typeof(TransactionBase), typeof(IntPtr)

                var d = typeof(TDelegate);
                var args = d.GetGenericArguments();
                var returnType = d.Name.Contains("Func") ? args.Last() : typeof(void);
                var parameterTypes = d.Name.Contains("Func") ? args.Take(args.Length - 1).ToArray() : args;

                var dm = new DynamicMethod("dm_" + nativeMethod.Name, returnType, parameterTypes, typeof(KeyValueStoreReplica).Module, true);
                var il = dm.GetILGenerator();
                emitBody(nativeMethod, il, parameterTypes);
                return (TDelegate)(object)dm.CreateDelegate(d);
            }

            static void EmitNonreadingMethod(MethodInfo nativeMethod, ILGenerator il, Type[] parameterTypes)
            {
                LoadStoreTxAndKey(il);

                var count = parameterTypes.Length;
                if (count > 3)
                {
                    il.Emit(OpCodes.Ldarg_3);
                }

                if (count > 4)
                {
                    il.Emit(OpCodes.Ldarg_S, (byte)4);
                }

                if (count > 5)
                {
                    il.Emit(OpCodes.Ldarg_S, (byte)5);
                }

                if (count > 6)
                {
                    throw new NotImplementedException();
                }

                il.EmitCall(OpCodes.Callvirt, nativeMethod, null);
                il.Emit(OpCodes.Ret);
            }

            static Func<KeyValueStoreReplica, TransactionBase, IntPtr, Func<RawItem, object>, object> BuildTryGet(MethodInfo tryGetMethod, DynamicMethod mapFromNativeToObject)
            {
                return Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, Func<RawItem, object>, object>>(tryGetMethod,
                    (mi, il, _) =>
                    {
                        il.DeclareLocal(InternalFabric.KeyValueStoreItemResultType);                        // 0, native result
                        il.DeclareLocal(InternalFabric.KeyValueStoreItemType.MakePointerType());            // 1, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM*
                        il.DeclareLocal(InternalFabric.KeyValueStoreItemMetadataType.MakePointerType());    // 2, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM_METADATA*
                        il.DeclareLocal(typeof(RawItem));                                                   // 3, RawItem

                        LoadStoreTxAndKey(il);

                        il.EmitCall(OpCodes.Callvirt, mi, null);
                        il.Emit(OpCodes.Ldarg_3);

                        // nativeResult, func
                        il.EmitCall(OpCodes.Call, mapFromNativeToObject, null);

                        // object
                        il.Emit(OpCodes.Ret);
                    });
            }

            static DynamicMethod BuildMappingFromNativeResultToObject()
            {
                var method = new DynamicMethod("dm_MapFromNativeResultTypeToObject", typeof(object),
                    new[] { InternalFabric.KeyValueStoreItemResultType, typeof(Func<RawItem, object>) },
                    typeof(KeyValueStoreReplica).Module, true);

                var il = method.GetILGenerator();
                il.DeclareLocal(InternalFabric.KeyValueStoreItemResultType);                        // 0, native result
                il.DeclareLocal(InternalFabric.KeyValueStoreItemType.MakePointerType());            // 1, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM*
                il.DeclareLocal(InternalFabric.KeyValueStoreItemMetadataType.MakePointerType());    // 2, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM_METADATA*
                il.DeclareLocal(typeof(RawItem));                                                   // 3, RawItem

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Stloc_0);

                var nonNullResult = il.DefineLabel();

                // if (result == null)
                //{
                //    return null;
                //}
                il.Emit(OpCodes.Ldloc_0);
                il.Emit(OpCodes.Brtrue_S, nonNullResult);
                il.Emit(OpCodes.Ldnull);
                il.Emit(OpCodes.Ret);

                il.MarkLabel(nonNullResult);

                // GC.KeepAlive(result);
                il.Emit(OpCodes.Ldloc_0);
                il.EmitCall(OpCodes.Call, typeof(GC).GetMethod("KeepAlive"), null);

                // nativeItemResult.get_Item()
                il.Emit(OpCodes.Ldloc_0);
                il.EmitCall(OpCodes.Callvirt, InternalFabric.KeyValueStoreItemResultType.GetMethod("get_Item"), null);

                il.EmitCall(OpCodes.Call, ReflectionHelpers.CastIntPtrToVoidPtr, null);
                il.Emit(OpCodes.Stloc_1);

                // empty stack, processing metadata
                il.Emit(OpCodes.Ldloc_1);   // NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM*
                il.Emit(OpCodes.Ldfld, InternalFabric.KeyValueStoreItemType.GetField("Metadata")); // IntPtr
                il.EmitCall(OpCodes.Call, ReflectionHelpers.CastIntPtrToVoidPtr, null); // void*
                il.Emit(OpCodes.Stloc_2);

                il.Emit(OpCodes.Ldloc_2); // NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM_METADATA*
                il.Emit(OpCodes.Ldfld, InternalFabric.KeyValueStoreItemMetadataType.GetField("Key")); // IntPtr

                il.Emit(OpCodes.Ldloc_2); // IntPtr, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM_METADATA*
                il.Emit(OpCodes.Ldfld, InternalFabric.KeyValueStoreItemMetadataType.GetField("ValueSizeInBytes")); // IntPtr, int

                il.Emit(OpCodes.Ldloc_2); // IntPtr, int, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM_METADATA*
                il.Emit(OpCodes.Ldfld, InternalFabric.KeyValueStoreItemMetadataType.GetField("SequenceNumber")); // IntPtr, int, long

                il.Emit(OpCodes.Ldloc_1); // IntPtr, int, long, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM*
                il.Emit(OpCodes.Ldfld, InternalFabric.KeyValueStoreItemType.GetField("Value")); // IntPtr (char*), int, long, IntPtr (byte*)

                // stack: IntPtr (char*), int, long, IntPtr (byte*)

                var ctor = typeof(RawItem).GetConstructors().Single(c => c.GetParameters().Length == 4);
                il.Emit(OpCodes.Newobj, ctor);

                // stack: rawItem
                il.Emit(OpCodes.Stloc_3);

                il.Emit(OpCodes.Ldarg_1);
                il.Emit(OpCodes.Ldloc_3);

                // stack: func, rawItem

                il.EmitCall(OpCodes.Callvirt, typeof(Func<RawItem, object>).GetMethod("Invoke"), null);

                // object
                il.Emit(OpCodes.Ret);

                return method;
            }

            static void LoadStoreTxAndKey(ILGenerator il)
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, InternalFabric.NativeStoreField); // nativeStore
                il.Emit(OpCodes.Ldarg_1); // nativeStore, tx
                il.EmitCall(OpCodes.Callvirt, InternalFabric.GetNativeTx, null); // nativeStore, nativeTx
                il.Emit(OpCodes.Ldarg_2); // nativeStore, nativeTx, key
            }
        }

        public unsafe struct RawItem
        {
            public RawItem(IntPtr key, int valueLength, long sequenceNumber, IntPtr value)
            {
                Key = (char*)key;
                ValueLength = valueLength;
                SequenceNumber = sequenceNumber;
                Value = (byte*)value;
            }

            public char* Key;
            public int ValueLength;
            public long SequenceNumber;
            public byte* Value;
        }
    }
}

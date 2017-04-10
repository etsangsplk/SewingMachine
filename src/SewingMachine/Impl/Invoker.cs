using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace SewingMachine.Impl
{
    class Invoker
    {
        // write
        public readonly Action<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr> Add;
        public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, sbyte> Contains;
        public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, sbyte, object> EnumerateByKey2;
        public readonly Func<IEnumerator<KeyValueStoreNotification>, object> GetNativeNotificationEnumerator2;
        public readonly Func<object, Func<RawItem, object>, object> NativeEnumeratorGetCurrent;
        public readonly Func<object, sbyte> NativeEnumeratorTryMoveNext;
        public readonly Func<object, Func<RawItem, object>, object> NativeNotificationEnumeratorGetCurrent;
        public readonly Func<object, sbyte> NativeNotificationEnumeratorTryMoveNext;
        public readonly Action<KeyValueStoreReplica, TransactionBase, IntPtr, long> Remove;
        public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, sbyte> TryAdd;
        public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, Func<RawItem, object>, object> TryGet;
        public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, long, sbyte> TryRemove;
        public readonly Func<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, long, sbyte> TryUpdate;
        public readonly Action<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, long> Update;

        public Invoker()
        {
            var methods = InternalFabric.NativeReplicaType.GetMethods().ToDictionary(mi => mi.Name);

            // Add
            // replica, tx, key, lenght, value
            Add = Utils.Build<Action<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr>>(methods["Add"],
                EmitNonreadingMethod);
            TryAdd =
                Utils.Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, sbyte>>(
                    methods["TryAdd"], EmitNonreadingMethod);

            // Update
            // replica, tx, key, lenght, value, sequenceNumber
            Update =
                Utils.Build<Action<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, long>>(
                    methods["Update"], EmitNonreadingMethod);
            TryUpdate =
                Utils.Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, int, IntPtr, long, sbyte>>(
                    methods["TryUpdate"], EmitNonreadingMethod);

            // Remove
            // replica, tx, key, sequenceNumber
            Remove = Utils.Build<Action<KeyValueStoreReplica, TransactionBase, IntPtr, long>>(methods["Remove"],
                EmitNonreadingMethod);
            TryRemove =
                Utils.Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, long, sbyte>>(methods["TryRemove"],
                    EmitNonreadingMethod);

            // Contains
            Contains = Utils.Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, sbyte>>(methods["Contains"],
                EmitNonreadingMethod);

            // TryGetValue
            var mapFromNativeToObject = BuildMappingFromNativeResultToObject();
            TryGet = BuildTryGet(methods["TryGet"], mapFromNativeToObject);

            // EnumerateByKey2
            EnumerateByKey2 =
                Utils.Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, sbyte, object>>(
                    methods["EnumerateByKey2"], EmitNonreadingMethod);

            // NativeEnumeratorMoveNext
            var enumerator =
                InternalFabric.NativeRuntimeType.DeclaredNestedTypes.Single(
                    t => t.Name == "IFabricKeyValueStoreItemEnumerator2");
            var enumeratorMethods = enumerator.GetMethods().ToDictionary(mi => mi.Name);
            NativeEnumeratorTryMoveNext = Utils.Build<Func<object, sbyte>>(enumeratorMethods["TryMoveNext"],
                (mi, il, parameterTypes) =>
                {
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Castclass, enumerator);
                    il.EmitCall(OpCodes.Callvirt, mi, null);
                    il.Emit(OpCodes.Ret);
                });

            NativeEnumeratorGetCurrent =
                Utils.Build<Func<object, Func<RawItem, object>, object>>(enumeratorMethods["get_Current"],
                    (mi, il, parameterTypes) =>
                    {
                        il.Emit(OpCodes.Ldarg_0);
                        il.Emit(OpCodes.Castclass, enumerator);
                        il.EmitCall(OpCodes.Callvirt, mi, null); // stack: IFabricKeyValueStoreItemResult
                        il.Emit(OpCodes.Ldarg_1); // stack: IFabricKeyValueStoreItemResult, mapper

                        il.EmitCall(OpCodes.Call, mapFromNativeToObject, null); // stack: object

                        il.Emit(OpCodes.Ret);
                    });

            // KeyValueStoreNotificationEnumerator

            var notificationEnumerator =
                typeof(KeyValueStoreNotification).Assembly.GetType(
                    "System.Fabric.KeyValueStoreNotificationEnumerator");
            var nativeNotificationEnumeratorField = notificationEnumerator.GetField("nativeEnumerator",
                ReflectionHelpers.AllInstance);
            var nativeNotificationEnumeratorType = nativeNotificationEnumeratorField.FieldType;

            GetNativeNotificationEnumerator2 =
                Utils.Build<Func<IEnumerator<KeyValueStoreNotification>, object>>(
                    "dm_getNativeNotificationEnumerator",
                    (il, types) =>
                    {
                        il.Emit(OpCodes.Ldarg_0);
                        il.Emit(OpCodes.Castclass, notificationEnumerator);
                        il.Emit(OpCodes.Ldfld, nativeNotificationEnumeratorField);
                        il.Emit(OpCodes.Ret);
                    });

            NativeNotificationEnumeratorTryMoveNext =
                Utils.Build<Func<object, sbyte>>(
                    nativeNotificationEnumeratorType.GetMethod("TryMoveNext", ReflectionHelpers.AllInstance),
                    (mi, il, types) =>
                    {
                        il.Emit(OpCodes.Ldarg_0);
                        il.Emit(OpCodes.Castclass, nativeNotificationEnumeratorType);
                        il.EmitCall(OpCodes.Callvirt, mi, null);
                        il.Emit(OpCodes.Ret);
                    });

            NativeNotificationEnumeratorGetCurrent =
                Utils.Build<Func<object, Func<RawItem, object>, object>>(
                    nativeNotificationEnumeratorType.GetMethod("get_Current"),
                    (mi, il, parameterTypes) =>
                    {
                        il.Emit(OpCodes.Ldarg_0);
                        il.Emit(OpCodes.Castclass, enumerator);
                        il.EmitCall(OpCodes.Callvirt, mi, null);
                        // stack:  NativeRuntime.IFabricKeyValueStoreNotification
                        il.Emit(OpCodes.Ldarg_1); // stack: IFabricKeyValueStoreItemResult, mapper

                        il.EmitCall(OpCodes.Call, mapFromNativeToObject, null); // stack: object

                        il.Emit(OpCodes.Ret);
                    });
        }

        static void EmitNonreadingMethod(MethodInfo nativeMethod, ILGenerator il, Type[] parameterTypes)
        {
            LoadStoreTxAndKey(il);

            var count = parameterTypes.Length;
            if (count > 3)
                il.Emit(OpCodes.Ldarg_3);

            if (count > 4)
                il.Emit(OpCodes.Ldarg_S, (byte) 4);

            if (count > 5)
                il.Emit(OpCodes.Ldarg_S, (byte) 5);

            if (count > 6)
                throw new NotImplementedException();

            il.EmitCall(OpCodes.Callvirt, nativeMethod, null);
            il.Emit(OpCodes.Ret);
        }

        static Func<KeyValueStoreReplica, TransactionBase, IntPtr, Func<RawItem, object>, object> BuildTryGet(
            MethodInfo tryGetMethod, MethodInfo mapFromNativeToObject)
        {
            return
                Utils.Build<Func<KeyValueStoreReplica, TransactionBase, IntPtr, Func<RawItem, object>, object>>(
                    tryGetMethod,
                    (mi, il, _) =>
                    {
                        il.DeclareLocal(InternalFabric.KeyValueStoreItemResultType); // 0, native result
                        il.DeclareLocal(InternalFabric.KeyValueStoreItemType.MakePointerType());
                        // 1, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM*
                        il.DeclareLocal(InternalFabric.KeyValueStoreItemMetadataType.MakePointerType());
                        // 2, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM_METADATA*
                        il.DeclareLocal(typeof(RawItem)); // 3, RawItem

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
                new[] {InternalFabric.KeyValueStoreItemResultType, typeof(Func<RawItem, object>)},
                typeof(KeyValueStoreReplica).Module, true);

            var il = method.GetILGenerator();
            il.DeclareLocal(InternalFabric.KeyValueStoreItemResultType); // 0, native result
            il.DeclareLocal(InternalFabric.KeyValueStoreItemType.MakePointerType());
            // 1, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM*
            il.DeclareLocal(InternalFabric.KeyValueStoreItemMetadataType.MakePointerType());
            // 2, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM_METADATA*
            il.DeclareLocal(typeof(RawItem)); // 3, RawItem

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
            il.Emit(OpCodes.Ldloc_1); // NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM*
            il.Emit(OpCodes.Ldfld, InternalFabric.KeyValueStoreItemType.GetField("Metadata")); // IntPtr
            il.EmitCall(OpCodes.Call, ReflectionHelpers.CastIntPtrToVoidPtr, null); // void*
            il.Emit(OpCodes.Stloc_2);

            il.Emit(OpCodes.Ldloc_2); // NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM_METADATA*
            il.Emit(OpCodes.Ldfld, InternalFabric.KeyValueStoreItemMetadataType.GetField("Key")); // IntPtr

            il.Emit(OpCodes.Ldloc_2); // IntPtr, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM_METADATA*
            il.Emit(OpCodes.Ldfld, InternalFabric.KeyValueStoreItemMetadataType.GetField("ValueSizeInBytes"));
            // IntPtr, int

            il.Emit(OpCodes.Ldloc_2); // IntPtr, int, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM_METADATA*
            il.Emit(OpCodes.Ldfld, InternalFabric.KeyValueStoreItemMetadataType.GetField("SequenceNumber"));
            // IntPtr, int, long

            il.Emit(OpCodes.Ldloc_1); // IntPtr, int, long, NativeTypes.FABRIC_KEY_VALUE_STORE_ITEM*
            il.Emit(OpCodes.Ldfld, InternalFabric.KeyValueStoreItemType.GetField("Value"));
            // IntPtr (char*), int, long, IntPtr (byte*)

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
}
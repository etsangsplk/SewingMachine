using System.Fabric;
using System.Linq;
using System.Reflection;

namespace SewingMachine
{
    static class InternalFabric
    {
        public static readonly TypeInfo NativeRuntimeType = typeof(KeyValueStoreReplica)
            .Assembly.DefinedTypes
            .Single(t => t.Name == "NativeRuntime");

        public static readonly TypeInfo NativeReplicaType = NativeRuntimeType
            .DeclaredNestedTypes
            .Single(t => t.Name == "IFabricKeyValueStoreReplica6");

        public static readonly FieldInfo NativeStoreField = typeof(KeyValueStoreReplica)
            .GetFields(ReflectionHelpers.AllInstance)
            .Single(f => f.FieldType == NativeReplicaType);

        public static readonly MethodInfo GetNativeTx = typeof(TransactionBase).GetProperty("NativeTransactionBase", ReflectionHelpers.AllInstance)
            .GetGetMethod(true);

        public static readonly TypeInfo NativeTypes = typeof(KeyValueStoreReplica)
            .Assembly.DefinedTypes
            .Single(t => t.Name == "NativeTypes");

        public static readonly TypeInfo KeyValueStoreItemResultType = NativeRuntimeType.DeclaredNestedTypes.Single(t => t.Name == "IFabricKeyValueStoreItemResult");

        /// <summary>
        /// Struct hidden in <see cref="KeyValueStoreItemResultType"/>
        /// </summary>
        public static readonly TypeInfo KeyValueStoreItemType = NativeTypes.DeclaredNestedTypes.Single(t => t.Name == "FABRIC_KEY_VALUE_STORE_ITEM");

        public static readonly TypeInfo KeyValueStoreItemMetadataType = NativeTypes.DeclaredNestedTypes.Single(t => t.Name == "FABRIC_KEY_VALUE_STORE_ITEM_METADATA");
    }
}
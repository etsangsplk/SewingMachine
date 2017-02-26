namespace SewingMachine
{
    /// <summary>
    /// Simple tuple holding the key and the value for <see cref="RawAccessorToKeyValueStoreReplica"/> opertions.
    /// </summary>
    public unsafe struct ReplicaKeyValue
    {
        public readonly char* Key;
        public readonly byte* Value;
        public readonly int Length;

        public ReplicaKeyValue(char* key, byte* value, int length)
        {
            Key = key;
            Value = value;
            Length = length;
        }
    }
}
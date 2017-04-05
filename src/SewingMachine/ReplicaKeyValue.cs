using System;

namespace SewingMachine
{
    /// <summary>
    /// Simple tuple holding the key and the value for <see cref="RawAccessorToKeyValueStoreReplica"/> opertions.
    /// </summary>
    public struct ReplicaKeyValue
    {
        public readonly IntPtr Key;
        public readonly IntPtr Value;
        public readonly int ValueLength;
        
        /// <param name="key">Unicode null ended string.</param>
        /// <param name="value">Byte array.</param>
        /// <param name="valueLength"></param>
        public ReplicaKeyValue(IntPtr key, IntPtr value, int valueLength)
        {
            Key = key;
            Value = value;
            ValueLength = valueLength;
        }
    }
}
using System;

namespace SewingMachine
{
    /// <summary>
    /// A key value unmanaged tuple.
    /// </summary>
    public struct RawItem
    {
        public RawItem(IntPtr key, int valueLength, long sequenceNumber, IntPtr value)
        {
            Key = key;
            ValueLength = valueLength;
            SequenceNumber = sequenceNumber;
            Value = value;
        }

        /// <summary>
        ///     Unicode null terminated string.
        /// </summary>
        public IntPtr Key;

        /// <summary>
        ///     Byte array
        /// </summary>
        public IntPtr Value;

        public int ValueLength;
        public long SequenceNumber;
    }
}
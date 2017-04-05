using System;
using NUnit.Framework;
using SewingMachine.Impl;

namespace SewingMachine.Tests
{
    public class InternalFabricTests
    {
        [Test]
        public void Test()
        {
            // just load type
            GC.KeepAlive(InternalFabric.NativeReplicaType);
        }
    }
}
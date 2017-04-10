using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Fabric;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Remoting.Runtime;
using SewingMachine;

namespace TestKeyValueStateful
{
    /// <summary>
    /// An instance of this class is created for each service replica by the Service Fabric runtime.
    /// </summary>
    sealed class TestKeyValueStateful : KeyValueStatefulService, ITestKeyValueStateful
    {
        static readonly IntPtr TestKey = Marshal.StringToHGlobalUni("test");

        public TestKeyValueStateful(StatefulServiceContext context)
            : base(context)
        { }

        /// <summary>
        /// Optional override to create listeners (e.g., HTTP, Service Remoting, WCF, etc.) for this service replica to handle client or user requests.
        /// </summary>
        /// <remarks>
        /// For more information on service communication, see https://aka.ms/servicefabricservicecommunication
        /// </remarks>
        /// <returns>A collection of listeners.</returns>
        protected override IEnumerable<ServiceReplicaListener> CreateServiceReplicaListeners()
        {
            return new[] { new ServiceReplicaListener(this.CreateServiceRemotingListener) };
        }

        /// <summary>
        /// This is the main entry point for your service replica.
        /// This method executes when this replica of your service becomes primary and has write status.
        /// </summary>
        /// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service replica.</param>
        protected override async Task RunAsync(CancellationToken cancellationToken)
        {
            ReplicaKeyValue rkv;
            var i = 0;

            unsafe
            {
                rkv = new ReplicaKeyValue(TestKey, new IntPtr((byte*)&i), 4);
            }

            using (var tx = OpenSession())
            {
                tx.Add(rkv);
                await tx.SaveChangesAsync();
            }

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                //var elapsed = await UpdateValue();
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
        }

        public async Task<TimeSpan> UpdateValue()
        {
            ReplicaKeyValue rkv;
            var i = 0;

            unsafe
            {
                var v = new IntPtr((byte*)&i);
                rkv = new ReplicaKeyValue(TestKey, v, 4);
            }

            var sw = Stopwatch.StartNew();
            using (var tx = OpenSession())
            {
                // ReSharper disable once ConvertClosureToMethodGroup
                var item = (Tuple<long, int>)tx.TryGet(TestKey, r => Parse(r));

                if (item == null)
                {
                    tx.Add(rkv);
                }
                else
                {
                    i = item.Item2 + 1;
                    tx.Update(rkv, item.Item1);
                }

                await tx.SaveChangesAsync();
            }

            sw.Stop();
            return sw.Elapsed;
        }

        static unsafe Tuple<long, int> Parse(RawItem r)
        {
            return r.Value != IntPtr.Zero ? Tuple.Create(r.SequenceNumber, *(int*)r.Value) : null;
        }
    }
}

using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Health;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Client;
using Microsoft.ServiceFabric.Services.Remoting.Client;
using NUnit.Framework;
using TestKeyValueStateful;

namespace SewingMachine.Tests
{
    public class KeyValueStatefulTests
    {
        [Test]
        public async Task UpdateMultipleTimes()
        {
            ServicePartitionKey partitionKey;
            using (var client = new FabricClient())
            {
                var partitions = await client.QueryManager.GetPartitionListAsync(SetupApp.TestKeyValueStatefulUri);
                var partition = (Int64RangePartitionInformation) partitions.First().PartitionInformation;
                partitionKey = new ServicePartitionKey(partition.LowKey);

                while (true)
                {
                    var replicas = await client.QueryManager.GetReplicaListAsync(partition.Id).ConfigureAwait(false);
                    if (replicas.Count == 3 && replicas.All(r => r.HealthState == HealthState.Ok))
                    {
                        break;
                    }

                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }

            var service = ServiceProxy.Create<ITestKeyValueStateful>(SetupApp.TestKeyValueStatefulUri, partitionKey);

            var spans = new List<TimeSpan>();

            for (var i = 0; i < 1000; i++)
            {
                spans.Add(await service.UpdateValue());
            }

            spans.Sort();
            Console.WriteLine($"Min time: {spans.First()}");
            Console.WriteLine($"Max time: {spans.Last()}");
        }
    }
}
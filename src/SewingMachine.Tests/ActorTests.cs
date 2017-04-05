using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors;
using Microsoft.ServiceFabric.Actors.Client;
using NUnit.Framework;
using NUnit.Framework.Interfaces;
using TestActor.Interfaces;

namespace SewingMachine.Tests
{
    public class ActorTests
    {
        [Timeout(60000)]
        [TestCaseSource(nameof(GetTestCases))]
        public Task TestActor(MethodInfo info)
        {
            var actorId = ActorId.CreateRandom();

            var actor = ActorProxy.Create<ITestActor>(actorId, SetupApp.ActorUri);

            return (Task)info.Invoke(actor, new object[] { CancellationToken.None });
        }

        public static IEnumerable<ITestCaseData> GetTestCases()
        {
            foreach (var method in typeof(ITestActor).GetMethods())
            {
                var testCaseData = new TestCaseData(method) { TestName = method.Name.Replace("_", " ") };
                yield return testCaseData;
            }
        }
    }
}
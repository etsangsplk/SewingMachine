using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Description;
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
        static readonly Uri ApplicationName = new Uri(@"fabric:/TestApp");
        static readonly Uri ActorUri = new Uri(@"fabric:/TestApp/TestActorService");
        const string ImageStorePath = "3b4f2d75acf24d02a34918ee4d7c08b6";
        const string ImageStoreConnectionString = @"file:C:\SfDevCluster\Data\ImageStoreShare";
        const string ApplicationTypeName = "TestAppType";
        const string ApplicationTypeVersion = "1.0.0";

        [OneTimeSetUp]
        public async Task SetUp()
        {
            using (var fabric = new FabricClient())
            {
                var app = fabric.ApplicationManager;
                app.CopyApplicationPackage(ImageStoreConnectionString, @"C:\GIT\SewingMachine\src\TestApp\pkg\Debug", ImageStorePath);
                await app.ProvisionApplicationAsync(ImageStorePath).ConfigureAwait(false);
                await app.CreateApplicationAsync(new ApplicationDescription(ApplicationName, ApplicationTypeName, ApplicationTypeVersion)).ConfigureAwait(false);
            }
        }

        [Timeout(60000)]
        [TestCaseSource(nameof(GetTestCases))]
        public Task TestActor(MethodInfo info)
        {
            var actorId = ActorId.CreateRandom();

            var actor = ActorProxy.Create<ITestActor>(actorId, ActorUri);

            return (Task)info.Invoke(actor, new object[] { CancellationToken.None });
        }

        [OneTimeTearDown]
        public async Task ServiceFabricTearDown()
        {
            using (var fabric = new FabricClient())
            {
                var app = fabric.ApplicationManager;
                await app.DeleteApplicationAsync(new DeleteApplicationDescription(ApplicationName)).ConfigureAwait(false);
                await app.UnprovisionApplicationAsync(ApplicationTypeName, ApplicationTypeVersion).ConfigureAwait(false);
                app.RemoveApplicationPackage(ImageStoreConnectionString, ImageStorePath);
            }
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
using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Description;
using System.IO;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization.Json;
using System.Threading.Tasks;
using System.Xml.Linq;
using NUnit.Framework;

namespace SewingMachine.Tests
{
    [SetUpFixture]
    public class SetupApp
    {
        public static readonly Uri ApplicationName = new Uri(@"fabric:/TestApp");
        public static readonly Uri ActorUri = new Uri(@"fabric:/TestApp/TestActorService");
        public static readonly Uri TestKeyValueStatefulUri = new Uri(@"fabric:/TestApp/TestKeyValueStateful");

        const string ImageStorePath = "3b4f2d75acf24d02a34918ee4d7c08b6";
        const string ApplicationTypeName = "TestAppType";
        const string ApplicationTypeVersion = "1.0.0";

        string imageStoreConnectionString;

        [OneTimeSetUp]
        public async Task SetUp()
        {
            var clusterManifest = await GetClusterManifest(new Uri("http://localhost:19080")).ConfigureAwait(false);
            imageStoreConnectionString = clusterManifest["Management"]["ImageStoreConnectionString"];

            var directoryName = "Release";
#if DEBUG
            directoryName = "Debug";
#endif

            var testAppPkgPath = Path.Combine(DetermineCallerFilePath(), $@"..\TestApp\pkg\{directoryName}");

            using (var fabric = new FabricClient())
            {
                var app = fabric.ApplicationManager;
                app.CopyApplicationPackage(imageStoreConnectionString, testAppPkgPath, ImageStorePath);
                await app.ProvisionApplicationAsync(ImageStorePath).ConfigureAwait(false);
                await app.CreateApplicationAsync(new ApplicationDescription(ApplicationName, ApplicationTypeName, ApplicationTypeVersion)).ConfigureAwait(false);
            }
        }

        [OneTimeTearDown]
        public async Task ServiceFabricTearDown()
        {
            using (var fabric = new FabricClient())
            {
                var app = fabric.ApplicationManager;
                await app.DeleteApplicationAsync(new DeleteApplicationDescription(ApplicationName)).ConfigureAwait(false);
                await app.UnprovisionApplicationAsync(ApplicationTypeName, ApplicationTypeVersion).ConfigureAwait(false);
                app.RemoveApplicationPackage(imageStoreConnectionString, ImageStorePath);
            }
        }

        static async Task<Dictionary<string, Dictionary<string, string>>> GetClusterManifest(Uri clusterUri)
        {
            using (var client = new HttpClient())
            using (var response = await client.GetStreamAsync(new Uri(clusterUri, "/$/GetClusterManifest?api-version=1.0")).ConfigureAwait(false))
            {
                var serializer = new DataContractJsonSerializer(typeof(ClusterManifest));
                var clusterManifest = (ClusterManifest) serializer.ReadObject(response);
                using (var reader = new StringReader(clusterManifest.Manifest))
                {

                    var document = XDocument.Load(reader);
                    XNamespace ns = document.Root.GetDefaultNamespace();
                    var sections = new Dictionary<string, Dictionary<string, string>>();
                    foreach (var section in document.Descendants(ns + "Section"))
                    {
                        var dictionary = new Dictionary<string, string>();

                        foreach (var parameter in section.Descendants(ns + "Parameter"))
                        {
                            dictionary.Add(parameter.Attribute("Name").Value, parameter.Attribute("Value").Value);
                        }

                        sections.Add(section.Attribute("Name").Value, dictionary);
                    }
                    return sections;
                }
            }
        }

        static string DetermineCallerFilePath([CallerFilePath] string path = null)
        {
            return Path.GetDirectoryName(path);
        }

        public class ClusterManifest
        {
            public string Manifest { get; set; }
        }
    }
}
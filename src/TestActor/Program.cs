using System;
using System.Threading;
using SewingMachine;

namespace TestActor
{
    static class Program
    {
        static void Main()
        {
            try
            {
                RawStatePersistentActor.RegisterActorAsync<TestActor>().GetAwaiter().GetResult();

                Thread.Sleep(Timeout.Infinite);
            }
            catch (Exception e)
            {
                ActorEventSource.Current.ActorHostInitializationFailed(e.ToString());
                throw;
            }
        }
    }
}

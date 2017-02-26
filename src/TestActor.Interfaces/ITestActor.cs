using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors;

namespace TestActor.Interfaces
{
    public interface ITestActor : IActor
    {
        Task When_accessing_StateManager_should_throw(CancellationToken cancellationToken);
        Task When_value_added_should_get_it(CancellationToken cancellationToken);
    }
}

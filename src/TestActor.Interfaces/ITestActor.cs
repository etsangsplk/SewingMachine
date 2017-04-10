﻿using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors;

namespace TestActor.Interfaces
{
    public interface ITestActor : IActor
    {
        Task When_accessing_StateManager_should_throw(CancellationToken cancellationToken);
        Task When_Add_should_Get_value(CancellationToken cancellationToken);
        Task When_TryRemove_non_existent_value_should_not_fail(CancellationToken cancellationToken);
        Task When_Remove_non_existent_value_should_throw(CancellationToken cancellationToken);
        Task When_Update_existent_value_should_replace(CancellationToken cancellationToken);
        Task When_TryUpdate_existent_value_with_wrong_version_should_throw(CancellationToken cancellationToken);
        Task When_TryAdd_should_fail_on_existing_key(CancellationToken cancellationToken);
        Task When_Enumerate_should_go_through_all_prefixed_value(CancellationToken cancellationToken);
        Task When_registering_BeforeSafe_should_execute_it_when_saving(CancellationToken cancellationToken);
    }
}

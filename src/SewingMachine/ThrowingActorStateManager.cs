using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Actors.Runtime;
using Microsoft.ServiceFabric.Data;

namespace SewingMachine
{
    class ThrowingActorStateManager : IActorStateManager
    {
        static readonly InvalidOperationException Ex = new InvalidOperationException("This actor state manager should not be used");

        public Task AddStateAsync<T>(string stateName, T value, CancellationToken cancellationToken)
        {
            throw Ex;
        }

        public Task<T> GetStateAsync<T>(string stateName, CancellationToken cancellationToken)
        {
            throw Ex;
        }

        public Task SetStateAsync<T>(string stateName, T value, CancellationToken cancellationToken)
        {
            throw Ex;
        }

        public Task RemoveStateAsync(string stateName, CancellationToken cancellationToken)
        {
            throw Ex;
        }

        public Task<bool> TryAddStateAsync<T>(string stateName, T value, CancellationToken cancellationToken)
        {
            throw Ex;
        }

        public Task<ConditionalValue<T>> TryGetStateAsync<T>(string stateName, CancellationToken cancellationToken)
        {
            throw Ex;
        }

        public Task<bool> TryRemoveStateAsync(string stateName, CancellationToken cancellationToken)
        {
            throw Ex;
        }

        public Task<bool> ContainsStateAsync(string stateName, CancellationToken cancellationToken)
        {
            throw Ex;
        }

        public Task<T> GetOrAddStateAsync<T>(string stateName, T value, CancellationToken cancellationToken)
        {
            throw Ex;
        }

        public Task<T> AddOrUpdateStateAsync<T>(string stateName, T addValue, Func<string, T, T> updateValueFactory,
            CancellationToken cancellationToken)
        {
            throw Ex;
        }

        public Task<IEnumerable<string>> GetStateNamesAsync(CancellationToken cancellationToken)
        {
            throw Ex;
        }

        public Task ClearCacheAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }

        public Task SaveStateAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(0);
        }
    }
}
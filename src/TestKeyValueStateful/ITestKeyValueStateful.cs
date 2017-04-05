using System;
using System.Threading.Tasks;
using Microsoft.ServiceFabric.Services.Remoting;

namespace TestKeyValueStateful
{
    public interface ITestKeyValueStateful : IService
    {
        Task<TimeSpan> UpdateValue();
    }
}
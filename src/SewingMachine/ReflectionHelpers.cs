using System;
using System.Linq;
using System.Reflection;

namespace SewingMachine
{
    static class ReflectionHelpers
    {
        public const BindingFlags AllInstance = BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance;
        public static readonly MethodInfo CastIntPtrToVoidPtr = typeof(IntPtr).GetMethods()
            .Single(m => m.ReturnType == typeof(void*) && m.GetParameters().Length == 1 && m.GetParameters()[0].ParameterType == typeof(IntPtr));
    }
}
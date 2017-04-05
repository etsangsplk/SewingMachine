using System;
using System.Fabric;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace SewingMachine.Impl
{
    static class Utils
    {
        public static MemcpyDelegate Memcpy;

        public unsafe delegate void MemcpyDelegate(byte* destination, byte* src, int length);

        static Utils()
        {
            var types = new[] { typeof(byte*), typeof(byte*), typeof(int) };
            var method = typeof(Buffer).GetMethod("Memcpy", BindingFlags.NonPublic | BindingFlags.Static, null, types, null);
            var dm = new DynamicMethod("dm_Memcpy", typeof(void), types, typeof(Buffer).Module, true);

            var il = dm.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Ldarg_2);
            il.EmitCall(OpCodes.Call, method, null);
            il.Emit(OpCodes.Ret);

            Memcpy = (MemcpyDelegate)dm.CreateDelegate(typeof(MemcpyDelegate));
        }

        public static Action<T1> BuildStatic<T1>(Type t, string methodName)
        {
            var method = t.GetMethods(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
                .Where(mi => mi.Name == methodName)
                .Single(mi =>
                {
                    var parameters = mi.GetParameters().Select(p => p.ParameterType).ToArray();
                    return parameters.Length == 1 && parameters[0] == typeof(T1);
                });

            return Build<Action<T1>>(method.Name, (il, argTypes) =>
            {
                il.Emit(OpCodes.Ldarg_0);
                il.EmitCall(OpCodes.Call, method, null);
                il.Emit(OpCodes.Ret);
            });
        }

        public static Action<T1, T2> BuildStatic<T1, T2>(Type t, string methodName)
        {
            var method = t.GetMethods(BindingFlags.Static | BindingFlags.NonPublic | BindingFlags.Public)
                .Where(mi => mi.Name == methodName)
                .Single(mi =>
                {
                    var parameters = mi.GetParameters().Select(p => p.ParameterType).ToArray();
                    return parameters.Length == 2 && parameters[0] == typeof(T1) && parameters[1] == typeof(T2);
                });

            return Build<Action<T1, T2>>(method.Name, (il, argTypes) =>
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldarg_1);
                il.EmitCall(OpCodes.Call, method, null);
                il.Emit(OpCodes.Ret);
            });
        }

        public static TDelegate Build<TDelegate>(MethodInfo method, Action<MethodInfo, ILGenerator, Type[]> emitBody)
        {
            return Build<TDelegate>(method.Name, (il, argTypes) => emitBody(method, il, argTypes));
        }

        public static TDelegate Build<TDelegate>(string name, Action<ILGenerator, Type[]> emitBody)
        {
            var d = typeof(TDelegate);
            var args = d.GetGenericArguments();
            var returnType = d.Name.Contains("Func") ? args.Last() : typeof(void);
            var parameterTypes = d.Name.Contains("Func") ? args.Take(args.Length - 1).ToArray() : args;

            var dm = new DynamicMethod("dm_" + name, returnType, parameterTypes, typeof(KeyValueStoreReplica).Module, true);
            var il = dm.GetILGenerator();
            emitBody(il, parameterTypes);
            return (TDelegate)(object)dm.CreateDelegate(d);
        }
    }
}
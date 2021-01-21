using System;
using System.Reflection;

namespace DataBoss
{
	public static class Lambdas
	{
		public static Action<T> Action<T>(string methodName) =>
			Create<Action<T>>(typeof(T), methodName);

		public static Func<T, TResult> Func<T, TResult>(string methodName) => 
			Create<Func<T,TResult>>(typeof(T), methodName);

		public static Func<T, TArg, TResult> Func<T, TArg, TResult>(string methodName) =>
			Create<Func<T, TArg, TResult>>(typeof(T), methodName, typeof(TArg));

		public static TDelegate CreateDelegate<TDelegate>(MethodInfo method) where TDelegate : Delegate =>
			(TDelegate)Delegate.CreateDelegate(typeof(TDelegate), null, method);

		public static TDelegate CreateDelegate<TDelegate>(object target, MethodInfo method) where TDelegate : Delegate =>
			(TDelegate)Delegate.CreateDelegate(typeof(TDelegate), target, method);

		static T Create<T>(Type type, string methodName, params Type[] types) where T : Delegate =>
			(T)Delegate.CreateDelegate(typeof(T), type.GetMethod(methodName, types));

		public static T Id<T>(T id) => id;
		public static T Default<T>() => default;
		public static void Nop<T>(T _) {}
	}
}

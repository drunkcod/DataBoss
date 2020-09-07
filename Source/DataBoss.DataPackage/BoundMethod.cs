using System;

namespace DataBoss.DataPackage
{
	static class BoundMethod
	{
		public static Action Bind<T>(Action<T> action, T arg) =>
			(Action)Delegate.CreateDelegate(typeof(Action), arg, action.Method);

		public static Func<TResult> Bind<T, TResult>(Func<T, TResult> func, T arg) =>
			(Func<TResult>)Delegate.CreateDelegate(typeof(Func<TResult>), arg, func.Method);

		public static Action<T2> Bind<T1, T2>(Action<T1, T2> action, T1 arg1) =>
			(Action<T2>)Delegate.CreateDelegate(typeof(Action<T2>), arg1, action.Method);
	}
}

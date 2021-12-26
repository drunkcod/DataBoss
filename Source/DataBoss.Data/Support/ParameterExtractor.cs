using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using DataBoss.Data.Scripting;

namespace DataBoss.Data.Support
{
	public class SqlDialect<TDialect, TCommand> where TDialect : ISqlDialect, new()
	{
		public static readonly ISqlDialect Instance;
		public static readonly DataBossScripter Scripter;

		static SqlDialect() {
			Instance = new TDialect();
			Scripter = new DataBossScripter(Instance);
		}
		protected static class Extractor<TArg>
		{
			public static Action<TCommand, TArg> CreateParameters =
				ToParams.CompileExtractor<TCommand, TArg>(Instance);
		}

		public static void AddParameters(TCommand command, object args) {
			if (args is null)
				return;
			Extractors.GetOrAdd(args.GetType(), CreateExtractor)(command, args);
		}

		static Action<TCommand, object> CreateExtractor(Type argsType) {
			var arg0 = Expression.Parameter(typeof(TCommand));
			var arg1 = Expression.Parameter(typeof(object));
			var addParameters = typeof(TDialect).GetMethods()
				.Single(x => x.Name == nameof(AddParameters) && x.IsGenericMethodDefinition);
			var body = Expression.Call(
				addParameters.MakeGenericMethod(argsType),
				arg0,
				Expression.Convert(arg1, argsType));
			return Expression.Lambda<Action<TCommand, object>>(body, arg0, arg1).Compile();
		}

		public static void AddParameters<T>(TCommand cmd, T args) =>
			Extractor<T>.CreateParameters(cmd, args);

		protected static readonly ConcurrentDictionary<Type, Action<TCommand, object>> Extractors = new();
	}
}

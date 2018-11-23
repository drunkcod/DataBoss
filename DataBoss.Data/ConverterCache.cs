using System;
using System.Collections.Concurrent;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public struct ConverterCacheKey : IEquatable<ConverterCacheKey>
	{
		readonly Type resultType;
		readonly string key;

		ConverterCacheKey(Type resultType, string key) {
			this.resultType = resultType;
			this.key = key; 
		}

		public override string ToString() => key ?? string.Empty;

		public static ConverterCacheKey Create<T>(T reader, Type result) where T : IDataReader => 
			new ConverterCacheKey(result, $"{typeof(T)}({FieldKey(reader)}) -> {result}");

		public static bool TryCreate(Type readerType, LambdaExpression e, out ConverterCacheKey key) { 
			var b = e.Body;
			if(b.NodeType == ExpressionType.New && (b is NewExpression c) && c.Arguments.All(x => x.NodeType == ExpressionType.Parameter)) {
				var args = string.Join(", ", 
					Enumerable.Range(0, c.Arguments.Count)
					.Select(x => $"{c.Arguments[x].Type} _{e.Parameters.IndexOf(c.Arguments[0] as ParameterExpression)}"));
				key = new ConverterCacheKey(e.Type, $"{readerType} -> .ctor({args})");
				return true;
			}
			key = default(ConverterCacheKey);
			return false;	
		}

		static string FieldKey(IDataReader reader) =>
			string.Join(", ", Enumerable.Range(0, reader.FieldCount).Select(ordinal => $"{reader.GetFieldType(ordinal)} [{reader.GetName(ordinal)}]"));

		public override int GetHashCode() => key.GetHashCode();
		public bool Equals(ConverterCacheKey other) => other.key == this.key && other.resultType == this.resultType;
	}

	public interface IConverterCache
	{
		DataRecordConverter GetOrAdd<TReader>(TReader reader, ConverterCacheKey key, Func<FieldMap, DataRecordConverter> createConverter) where TReader : IDataReader;
	}

	public class NullConverterCache : IConverterCache
	{
		NullConverterCache() { }

		public static IConverterCache Instance = new NullConverterCache();

		public DataRecordConverter GetOrAdd<TReader>(TReader reader, ConverterCacheKey result, Func<FieldMap, DataRecordConverter> createConverter) where TReader : IDataReader =>
			createConverter(FieldMap.Create(reader));
	}

	public class ConcurrentConverterCache : IConverterCache
	{
		readonly ConcurrentDictionary<string, DataRecordConverter> converterCache = new ConcurrentDictionary<string, DataRecordConverter>(); 

		public DataRecordConverter GetOrAdd<TReader>(TReader reader, ConverterCacheKey key, Func<FieldMap, DataRecordConverter> createConverter) where TReader : IDataReader { 
			if(!converterCache.TryGetValue(key.ToString(), out var found)) { 
				found = NullConverterCache.Instance.GetOrAdd(reader, key, createConverter);
				converterCache.TryAdd(key.ToString(), found);
			}
			return found;
		}

		static string FieldKey(IDataReader reader) =>
			string.Join(", ", Enumerable.Range(0, reader.FieldCount).Select(ordinal => $"{reader.GetFieldType(ordinal)} [{reader.GetName(ordinal)}]"));
	}
}
using System;
using System.Collections.Concurrent;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	public interface IConverterCache
	{
		DataRecordConverter GetOrAdd<TReader>(TReader reader, Type result, Func<FieldMap, Type, LambdaExpression> createConverter) where TReader : IDataReader;
	}

	public class NullConverterCache : IConverterCache
	{
		NullConverterCache() { }

		public static IConverterCache Instance = new NullConverterCache();

		public DataRecordConverter GetOrAdd<TReader>(TReader reader, Type result, Func<FieldMap, Type, LambdaExpression> createConverter) where TReader : IDataReader =>
			new DataRecordConverter(createConverter(FieldMap.Create(reader), result));
	}

	public class ConcurrentConverterCache : IConverterCache
	{
		readonly ConcurrentDictionary<string, DataRecordConverter> converterCache = new ConcurrentDictionary<string, DataRecordConverter>(); 

		public DataRecordConverter GetOrAdd<TReader>(TReader reader, Type result, Func<FieldMap, Type, LambdaExpression> createConverter) where TReader : IDataReader =>
			converterCache.GetOrAdd($"{typeof(TReader)}({FieldKey(reader)}) -> {result}", _ => NullConverterCache.Instance.GetOrAdd(reader, result, createConverter));

		static string FieldKey(IDataReader reader) =>
			string.Join(", ", Enumerable.Range(0, reader.FieldCount).Select(ordinal => $"{reader.GetFieldType(ordinal)} [{reader.GetName(ordinal)}]"));
	}
}
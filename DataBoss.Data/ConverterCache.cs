using System;
using System.Collections.Concurrent;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace DataBoss.Data
{
	public struct ConverterCacheKey : IEquatable<ConverterCacheKey>
	{
		ConverterCacheKey(Type resultType, string key) {
			this.ResultType = resultType;
			this.Key = key; 
		}

		public Type ResultType { get; private set; }
		public string Key { get; private set; }

		public override string ToString() => Key;

		public static ConverterCacheKey Create(IDataReader reader, Type result) =>
			new ConverterCacheKey(result, FormatReader(reader, "⇒ ").ToString());

		public static ConverterCacheKey Into(IDataReader reader, Type result) =>
			new ConverterCacheKey(result, FormatReader(reader, "↻ ").ToString());

		static StringBuilder FormatReader(IDataReader reader, string prefix = null) {
			var key = new StringBuilder(prefix, 128);
			key.Append(reader.GetType()).Append('(');
			return FieldKey(key, reader).Append(")");
		}

		public static ConverterCacheKey Create<T>(T reader, Delegate exemplar) where T : IDataReader =>
			new ConverterCacheKey(exemplar.Method.ReturnType, $"{typeof(T)}({FieldKey(reader)})->Invoke({ParameterKey(exemplar)})");

		public static bool TryCreate<T>(T reader, LambdaExpression e, out ConverterCacheKey key) where T : IDataReader { 
			var b = e.Body;
			if(b.NodeType == ExpressionType.New && (b is NewExpression c) && c.Arguments.All(x => x.NodeType == ExpressionType.Parameter)) {
				var args = string.Join(", ", 
					Enumerable.Range(0, c.Arguments.Count)
					.Select(x => $"{c.Arguments[x].Type} _{e.Parameters.IndexOf(c.Arguments[0] as ParameterExpression)}"));
				key = new ConverterCacheKey(e.Type, $"{typeof(T)}({FieldTypeKey(reader)})->.ctor({args})");
				return true;
			}
			key = default(ConverterCacheKey);
			return false;	
		}

		static string FieldKey(IDataReader reader) =>
			FieldKey(new StringBuilder(128), reader).ToString();

		static StringBuilder FieldKey(StringBuilder sb, IDataReader reader) {
			if (reader.FieldCount == 0)
				return sb;
			var nullable = GetNullability(reader);
			FormatField(sb, reader.GetName(0), NullableT(reader.GetFieldType(0), nullable[0]));
			for (var i = 1; i != reader.FieldCount; ++i)
				FormatField(sb.Append(", "), reader.GetName(i), NullableT(reader.GetFieldType(i), nullable[i]));

			return sb;
		}

		static bool[] GetNullability(IDataReader reader) {
			if(reader.FieldCount == 0)
				return Empty<bool>.Array;
			var r = new bool[reader.FieldCount];
			var schema = reader.GetSchemaTable();
			var allowDBNull = schema.Columns["AllowDBNull"];

			for (var i = 0; i != r.Length; ++i) {
				var row = schema.Rows[i];
				r[i] = (bool)row[allowDBNull];
			}

			return r;
		}

		static void FormatField(StringBuilder sb, string columnName, Type columnType) =>
			sb.Append(columnType).Append(" [").Append(columnName).Append(']');

		static Type NullableT(Type type, bool allowNull) =>
			type.IsPrimitive && allowNull ? typeof(Nullable<>).MakeGenericType(type) : type;

		static string FieldTypeKey(IDataReader reader) =>
			string.Join(", ", Enumerable.Range(0, reader.FieldCount).Select(ordinal => $"{reader.GetFieldType(ordinal)}"));

		static string ParameterKey(Delegate exemplar) =>
			string.Join(", ", exemplar.Method.GetParameters().Select(x => $"{x.ParameterType} {x.Name}"));

		public override int GetHashCode() => ResultType.GetHashCode();
		public override bool Equals(object obj) => Equals((ConverterCacheKey)obj);
		public bool Equals(ConverterCacheKey other) => other.Key == this.Key && other.ResultType == this.ResultType;
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
		readonly ConcurrentDictionary<ConverterCacheKey, DataRecordConverter> converterCache = new ConcurrentDictionary<ConverterCacheKey, DataRecordConverter>(); 

		public DataRecordConverter GetOrAdd<TReader>(TReader reader, ConverterCacheKey key, Func<FieldMap, DataRecordConverter> createConverter) where TReader : IDataReader { 
			if(!converterCache.TryGetValue(key, out var found)) { 
				found = createConverter(FieldMap.Create(reader));
				converterCache.TryAdd(key, found);
			}
			return found;
		}
	}
}
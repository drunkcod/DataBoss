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

		public static ConverterCacheKey Create(IDataReader reader, Type readerType, Type resultType) =>
			new(resultType, FormatReader(reader, readerType, "⇒").ToString());

		public static ConverterCacheKey Into(IDataReader reader, Type readerType, Type result) =>
			new(result, FormatReader(reader, readerType, "↻").ToString());

		static StringBuilder FormatReader(IDataReader reader, Type readerType, string prefix = null) {
			var key = new StringBuilder(prefix, 128);
			key.Append(readerType).Append('(');
			return FieldKey(key, reader).Append(')');
		}

		public static ConverterCacheKey Create<T>(T reader, Delegate exemplar) where T : IDataReader =>
			new(exemplar.Method.ReturnType, $"{typeof(T)}({FieldKey(reader)})⇒Invoke({ParameterKey(exemplar)})");

		public static bool TryCreate<T>(T reader, LambdaExpression e, out ConverterCacheKey key) where T : IDataReader { 
			var b = e.Body;
			if(b.NodeType == ExpressionType.New && (b is NewExpression c)) {
				var keyBuilder = new StringBuilder($"{typeof(T)}({FieldTypeKey(reader)})⇒.ctor(");

				var sep = "$";
				for(var i = 0; i != c.Arguments.Count; ++i) {
					if(c.Arguments[i] is not ParameterExpression p)
						goto nope;
					keyBuilder.Append(sep).Append(e.Parameters.IndexOf(p));
					sep = ", $";
				}

				key = new(b.Type, keyBuilder.Append(')').ToString());
				return true;
			}
			nope: key = default;
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

			if(schema.Rows.Count != reader.FieldCount)
				throw new InvalidOperationException("GetSchemaTable result doesn't match FieldCount.");

			for (var i = 0; i != r.Length; ++i) {
				var row = schema.Rows[i];
				r[i] = IfDbNull(row[allowDBNull], true);
			}

			return r;
		}

		static T IfDbNull<T>(object value, T whenNull) => value is DBNull ? whenNull : (T)value;

		static void FormatField(StringBuilder sb, string columnName, Type columnType) =>
			sb.Append(columnType).Append(" [").Append(columnName).Append(']');

		static Type NullableT(Type type, bool allowNull) =>
			type.IsPrimitive && allowNull ? typeof(Nullable<>).MakeGenericType(type) : type;

		static string FieldTypeKey(IDataReader reader) =>
			string.Join(", ", Enumerable.Range(0, reader.FieldCount).Select(ordinal => $"{reader.GetFieldType(ordinal)} ${ordinal}"));

		static string ParameterKey(Delegate exemplar) =>
			string.Join(", ", exemplar.Method.GetParameters().Select(x => $"{x.ParameterType} {x.Name}"));

		public override int GetHashCode() => ResultType.GetHashCode();
		
		public override bool Equals(object obj) => Equals((ConverterCacheKey)obj);
		
		public bool Equals(ConverterCacheKey other) => other.Key == this.Key && other.ResultType == this.ResultType;

		public static bool operator ==(ConverterCacheKey left, ConverterCacheKey right) => left.Equals(right);

		public static bool operator !=(ConverterCacheKey left, ConverterCacheKey right) => !(left == right);
	}

	public interface IConverterCache
	{
		DataRecordConverter GetOrAdd<TReader>(TReader reader, ConverterCacheKey key, Func<FieldMap, DataRecordConverter> createConverter) where TReader : IDataReader;
	}

	public class NullConverterCache : IConverterCache
	{
		NullConverterCache() { }

		public static readonly IConverterCache Instance = new NullConverterCache();

		public DataRecordConverter GetOrAdd<TReader>(TReader reader, ConverterCacheKey result, Func<FieldMap, DataRecordConverter> createConverter) where TReader : IDataReader =>
			createConverter(FieldMap.Create(reader));
	}

	public class ConcurrentConverterCache : IConverterCache
	{
		readonly ConcurrentDictionary<ConverterCacheKey, DataRecordConverter> converterCache = new(); 

		public DataRecordConverter GetOrAdd<TReader>(TReader reader, ConverterCacheKey key, Func<FieldMap, DataRecordConverter> createConverter) where TReader : IDataReader { 
			if(!converterCache.TryGetValue(key, out var found)) { 
				found = createConverter(FieldMap.Create(reader));
				converterCache.TryAdd(key, found);
			}
			return found;
		}
	}
}
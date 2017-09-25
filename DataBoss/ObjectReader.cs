using System;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;

namespace DataBoss
{
	public static class ObjectReader
	{
		public static ObjectReader<TReader> For<TReader>(TReader reader) where TReader : IDataReader =>
			new ObjectReader<TReader>(reader);

		public static Func<TReader, T> GetConverter<TReader, T>(TReader reader, ConverterCollection customConversions) where TReader : IDataReader => 
			MakeConverter<TReader, T>(reader, customConversions).Compile();

		public static Expression<Func<TReader, T>> MakeConverter<TReader, T>(TReader reader) where TReader : IDataReader =>
			MakeConverter<TReader, T>(reader, null);

		public static Expression<Func<TReader, T>> MakeConverter<TReader, T>(TReader reader, ConverterCollection customConversions) where TReader : IDataReader =>
			(Expression<Func<TReader, T>>)new ConverterFactory(typeof(TReader), customConversions, NullConverterCache.Instance).Converter(FieldMap.Create(reader), typeof(T));
	}

	public struct ObjectReader<TReader> : IDisposable where TReader : IDataReader
	{
		readonly TReader reader;

		public ObjectReader(TReader reader) { 
			this.reader = reader; 
		}

		void IDisposable.Dispose() => reader.Dispose();

		public ConvertingObjectReader<TReader> WithConverter<TFrom, TTo>(Func<TFrom, TTo> convert) =>
			new ConvertingObjectReader<TReader>(this).WithConverter(convert);

		public IEnumerable<T> Read<T>() => Read<T>((ConverterCollection)null);
		public IEnumerable<T> Read<T>(ConverterCollection converters) {
			var converter = GetConverter<T>(converters);
			while(reader.Read())
				yield return converter(reader);
		}

		public void Read<T>(Action<T> handleItem) => Read(null, handleItem);
		public void Read<T>(ConverterCollection converters, Action<T> handleItem) {
			var converter = GetConverter<T>(converters);
			while(reader.Read())
				handleItem(converter(reader));
		}

		Func<TReader, T> GetConverter<T>(ConverterCollection converters) => ObjectReader.GetConverter<TReader, T>(reader, converters);

		public bool NextResult() => reader.NextResult();
	}

	public class ConvertingObjectReader<TReader> : IDisposable where TReader : IDataReader
	{
		readonly ObjectReader<TReader> reader;
		readonly ConverterCollection customConversions;

		public ConvertingObjectReader(ObjectReader<TReader> reader) { 
			this.reader = reader; 
			this.customConversions = new ConverterCollection();
		}

		void IDisposable.Dispose() => (reader as IDisposable).Dispose();

		public ConvertingObjectReader<TReader> WithConverter<TFrom, TTo>(Func<TFrom, TTo> convert) {
			customConversions.Add(convert);
			return this;
		}

		public IEnumerable<T> Read<T>() => reader.Read<T>(customConversions);
		public void Read<T>(Action<T> handleItem) => reader.Read(customConversions, handleItem);

		public bool NextResult() => reader.NextResult();
	}
}
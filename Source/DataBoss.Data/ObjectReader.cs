using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Reflection;

namespace DataBoss.Data
{
	public static class ObjectReader
	{
		delegate IEnumerator<T> ItemReader<T>(IDataReader reader, ConverterCollection converters);

		static class ItemReaderCache<T>
		{
			static readonly ConcurrentDictionary<Type, ItemReader<T>> readers = new();

			public static ItemReader<T> GetReader(Type readerType) => readers.GetOrAdd(readerType, x => MakeReaderOfT<T>(x));
		}

		public static ObjectReader<TReader> For<TReader>(Func<TReader> reader) where TReader : IDataReader => new(reader);

		public static Func<TReader, T> GetConverter<TReader, T>(TReader reader) where TReader : IDataReader =>
			GetConverter<TReader, T>(reader, null);

		public static Func<TReader, T> GetConverter<TReader, T>(TReader reader, ConverterCollection customConversions) where TReader : IDataReader => 
			ConverterFactory(customConversions).GetConverter<TReader, T>(reader).Compiled;

		public static Expression<Func<TReader, T>> MakeConverter<TReader, T>(TReader reader) where TReader : IDataReader =>
			MakeConverter<TReader, T>(reader, null);

		public static Expression<Func<TReader, T>> MakeConverter<TReader, T>(TReader reader, ConverterCollection customConversions) where TReader : IDataReader =>
			ConverterFactory(customConversions).GetConverter<TReader, T>(reader).Expression;

		public static IEnumerator<T> Read<T>(this IDataReader reader) => 
			Read<T>(reader, null);

		public static IEnumerator<T> Read<T>(this IDataReader reader, ConverterCollection converters) =>
			ItemReaderCache<T>.GetReader(reader.GetType())(reader, converters);

		public static void Read<TReader, T>(this TReader reader, Action<T> handleItem) where TReader : IDataReader =>
			Read(reader, handleItem, null);

		public static void Read<TReader, T>(this TReader reader, Action<T> handleItem, ConverterCollection converters) where TReader : IDataReader {
			using var items = new ConvertingEnumerator<TReader, T>(reader, GetConverter<TReader, T>(reader, converters));
			while (items.MoveNext())
				handleItem(items.Current);
		}

		public static IEnumerable<T> Enumerable<T>(Func<IDataReader> getData, ConverterCollection converters) {

			using var items = Read<T>(getData(), converters);
			while (items.MoveNext())
				yield return items.Current;
		}

		static ItemReader<T> MakeReaderOfT<T>(Type readerType) {
			var m = typeof(ObjectReader).GetMethod(nameof(ReaderOfT), BindingFlags.NonPublic | BindingFlags.Static);
			return Lambdas.CreateDelegate<ItemReader<T>>(m.MakeGenericMethod(readerType, typeof(T)));
		}

		static IEnumerator<T> ReaderOfT<TReader, T>(IDataReader reader, ConverterCollection converters) where TReader : IDataReader {
			var readerOfT = (TReader)reader;
			return new ConvertingEnumerator<TReader, T>(readerOfT, GetConverter<TReader, T>(readerOfT, converters));
		}

		static ConverterFactory ConverterFactory(ConverterCollection customConversions) => new(customConversions, NullConverterCache.Instance);
	}

	public struct ObjectReader<TReader> where TReader : IDataReader
	{
		readonly Func<TReader> getReader;

		public ObjectReader(Func<TReader> getReader) { 
			this.getReader = getReader; 
		}

		public ConvertingObjectReader<TReader> WithConverter<TFrom, TTo>(Func<TFrom, TTo> convert) =>
			new ConvertingObjectReader<TReader>(this).WithConverter(convert);

		public IEnumerator<T> Read<T>() => Read<T>((ConverterCollection)null);

		public IEnumerator<T> Read<T>(ConverterCollection converters) {
			var reader = getReader();
			return new ConvertingEnumerator<TReader, T>(reader, GetConverter<T>(reader, converters));
		}

		public IEnumerator<T> Read<T>(ConverterFactory converters) {
			var reader = getReader();
			return new ConvertingEnumerator<TReader, T>(reader, converters.GetConverter<TReader, T>(reader).Compiled);
		}

		public void Read<T>(Action<T> handleItem) => Read(null, handleItem);

		public void Read<T>(ConverterCollection converters, Action<T> handleItem) {
			using var reader = getReader();
			Func<TReader, T> converter = GetConverter<T>(reader, converters);
			while (reader.Read())
				handleItem(converter(reader));
		}

		private static Func<TReader, T> GetConverter<T>(TReader reader, ConverterCollection converters) {
			return ObjectReader.GetConverter<TReader, T>(reader, converters);
		}
	}

	sealed class ConvertingEnumerator<TReader, T> : IEnumerator<T> where TReader : IDataReader
	{
		readonly TReader reader;
		readonly Func<TReader, T> convert;

		public ConvertingEnumerator(TReader reader, Func<TReader, T> convert) {
			this.reader = reader;
			this.convert = convert;
		}

		public T Current { get; private set; }
		object IEnumerator.Current => Current;

		void IDisposable.Dispose() => reader.Dispose();

		public bool MoveNext() {
			if (!reader.IsClosed && reader.Read()) {
				Current = convert(reader);
				return true;
			}
			else {
				reader.Close();
				Current = default;
				return false;
			}
		}

		public void Reset() => throw new NotSupportedException();
	}

	public sealed class ConvertingObjectReader<TReader> where TReader : IDataReader
	{
		readonly ObjectReader<TReader> reader;
		readonly ConverterCollection customConversions;

		public ConvertingObjectReader(ObjectReader<TReader> reader) { 
			this.reader = reader; 
			this.customConversions = new ConverterCollection();
		}

		public ConvertingObjectReader<TReader> WithConverter<TFrom, TTo>(Func<TFrom, TTo> convert) {
			customConversions.Add(convert);
			return this;
		}

		public IEnumerator<T> Read<T>() => reader.Read<T>(customConversions);
		public void Read<T>(Action<T> handleItem) => reader.Read(customConversions, handleItem);
	}
}
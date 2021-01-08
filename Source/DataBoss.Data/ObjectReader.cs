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
		static readonly ConcurrentDictionary<(Type Reader, Type T), Func<IDataReader, IEnumerable>> readerCache = new ConcurrentDictionary<(Type Reader, Type T), Func<IDataReader, IEnumerable>>();

		public static ObjectReader<TReader> For<TReader>(TReader reader) where TReader : IDataReader =>
			new ObjectReader<TReader>(reader);

		public static Func<TReader, T> GetConverter<TReader, T>(TReader reader) where TReader : IDataReader =>
			GetConverter<TReader, T>(reader, null);

		public static Func<TReader, T> GetConverter<TReader, T>(TReader reader, ConverterCollection customConversions) where TReader : IDataReader => 
			ConverterFactory(customConversions).GetConverter<TReader, T>(reader).Compiled;

		public static Expression<Func<TReader, T>> MakeConverter<TReader, T>(TReader reader) where TReader : IDataReader =>
			MakeConverter<TReader, T>(reader, null);

		public static Expression<Func<TReader, T>> MakeConverter<TReader, T>(TReader reader, ConverterCollection customConversions) where TReader : IDataReader =>
			ConverterFactory(customConversions).GetConverter<TReader, T>(reader).Expression;

		public static IEnumerable<T> Read<T>(this IDataReader reader) {
			var key = (reader.GetType(), typeof(T));
			var read = readerCache.GetOrAdd(key, MakeReaderOfT);
			return (IEnumerable<T>)read(reader);
		}

		static Func<IDataReader, IEnumerable> MakeReaderOfT((Type Reader, Type T) key) {
			var m = typeof(ObjectReader).GetMethod(nameof(ReaderOfT), BindingFlags.NonPublic | BindingFlags.Static);
			return (Func<IDataReader, IEnumerable>)Delegate.CreateDelegate(
				typeof(Func<IDataReader, IEnumerable>),
				m.MakeGenericMethod(key.Reader, key.T));
		}

		static IEnumerable ReaderOfT<TReader, T>(IDataReader reader) where TReader : IDataReader =>
			For((TReader)reader).Read<T>();

		static ConverterFactory ConverterFactory(ConverterCollection customConversions) => 
			new ConverterFactory(customConversions, NullConverterCache.Instance);
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
		
		public IEnumerable<T> Read<T>(ConverterCollection converters) => 
			new ConvertingEnumerable<T>(reader, GetConverter<T>(converters));

		public IEnumerable<T> Read<T>(ConverterFactory converters) =>
			new ConvertingEnumerable<T>(reader, converters.GetConverter<TReader, T>(reader).Compiled);

		class ConvertingEnumerable<T> : IEnumerable<T>, IEnumerator<T>
		{
			readonly TReader reader;
			readonly Func<TReader, T> convert;

			public ConvertingEnumerable(TReader reader, Func<TReader, T> convert) {
				this.reader = reader;
				this.convert = convert;
			}
				 
			public T Current { get; private set; }
			object IEnumerator.Current => Current;

			public IEnumerator<T> GetEnumerator() => this;
			IEnumerator IEnumerable.GetEnumerator() => this;

			public void Dispose() { }

			public bool MoveNext() {
				if(reader.Read()) {
					Current = convert(reader);
					return true;
				} else {
					Current = default;
					return false;
				}
			}

			public void Reset() => throw new NotSupportedException();
		}

		public void Read<T>(Action<T> handleItem) => Read(null, handleItem);
		public void Read<T>(ConverterCollection converters, Action<T> handleItem) {
			var converter = GetConverter<T>(converters);
			while(reader.Read())
				handleItem(converter(reader));
		}

		Func<TReader, T> GetConverter<T>(ConverterCollection converters) => 
			ObjectReader.GetConverter<TReader, T>(reader, converters);

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
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using CheckThat;
using Newtonsoft.Json;
using Xunit;

namespace DataBoss.DataPackage
{
	public class ItemOrArrayJsonConverterSpec
	{
		class WithItemOrArrayValue<T>
		{
			[JsonProperty("value")]
			[JsonConverter(typeof(ItemOrArrayJsonConverter))]
			public T Value { get; set; }
		}

		[Fact]
		public void single_item() => Check.That(
			() => ToItemOrArray("hello") == ToJson("hello"),
			() => FromJson<List<int>>(ToItemOrArray(42)).SequenceEqual(new[]{ 42 }));

		[Fact]
		public void multi_item() => Check.That(
			() => ToItemOrArray("hello", "world") == ToJson(new[]{ "hello", "world" }),
			() => FromJson<List<int>>(ToItemOrArray(1, 2, 3)).SequenceEqual(new[] { 1, 2, 3 }));

		[Fact]
		public void ICollection_of_T() => Check.That(
			() => FromJson<ICollection<int>>(ToItemOrArray(321)).SequenceEqual(new[] { 321 }));

		[Fact]
		public void Array_of_T() => Check.That(
			() => FromJson<int[]>(ToItemOrArray(321)).SequenceEqual(new[] { 321 }));

		[Fact]
		public void dispose_enumerators() {
			var items = new MyCollection<string> { "Hello World!" };
			var enumeratorCreated = false;
			var enumeratorDisposed = false;
			items.EnumeratorCreated += xs => {
				enumeratorCreated = true;
				xs.Disposed += (_,__) => enumeratorDisposed = true;
			};

			ToJson(items);
			Check.That(() => enumeratorCreated, () => enumeratorDisposed);
		}

		class MyCollection<T> : ICollection<T>
		{
			readonly List<T> items = new List<T>();

			public event Action<MyEnumerator> EnumeratorCreated;

			public int Count => items.Count;
			public bool IsReadOnly => false;

			public void Add(T item) => items.Add(item);
			public void Clear() => items.Clear();
			public bool Contains(T item) => items.Contains(item);
			public void CopyTo(T[] array, int arrayIndex) => items.CopyTo(array, arrayIndex);

			public IEnumerator<T> GetEnumerator() {
				var e = new MyEnumerator(items.GetEnumerator());
				EnumeratorCreated?.Invoke(e);
				return e;
			}

			public bool Remove(T item) => items.Remove(item);

			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

			public class MyEnumerator : IEnumerator<T>
			{
				readonly IEnumerator<T> enumerator;
				
				public MyEnumerator(IEnumerator<T> enumerator) {
					this.enumerator = enumerator;
				}

				public event EventHandler Disposed;

				public T Current => enumerator.Current;
				object IEnumerator.Current => Current;

				public void Dispose() {
					enumerator.Dispose();
					Disposed?.Invoke(this, EventArgs.Empty);
				}

				public bool MoveNext() => enumerator.MoveNext();

				public void Reset() => enumerator.Reset();
			}
		}

		static string ToJson(object value) => JsonConvert.SerializeObject(value);

		static string ToItemOrArray(params object[] values) {
			var r = new StringWriter();
			new ItemOrArrayJsonConverter().WriteJson(
				new JsonTextWriter(r), values, new JsonSerializer());
			return r.ToString();
		}
		static T FromJson<T>(string json) {
			return (T)new ItemOrArrayJsonConverter().ReadJson(
				new JsonTextReader(new StringReader(json)), 
				typeof(T), 
				null, 
				new JsonSerializer());
		}
	}
}

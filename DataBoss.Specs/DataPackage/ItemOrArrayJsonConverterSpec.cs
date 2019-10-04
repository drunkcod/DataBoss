using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Cone;
using DataBoss.DataPackage;
using Newtonsoft.Json;

namespace DataBoss.Specs.DataPackage
{
	[Describe(typeof(ItemOrArrayJsonConverter))]
	public class ItemOrArrayJsonConverterSpec
	{
		class WithItemOrArrayValue<T>
		{
			[JsonProperty("value")]
			[JsonConverter(typeof(ItemOrArrayJsonConverter))]
			public T Value;
		}

		WithItemOrArrayValue<T> Value<T>(T value) => new WithItemOrArrayValue<T> { Value = value };

		public void single_item() => Check.That(
			() => ToJson(Value(new[]{ "hello" })) == ToJson(new { value = "hello" }),
			() => FromJson<List<int>>(ToJson(new { value = 42 })).Value.SequenceEqual(new[]{ 42 }));

		public void multi_item() => Check.That(
			() => ToJson(Value(new[] { "hello", "world" })) == ToJson(new { value = new[]{ "hello", "world" } }),
			() => FromJson<List<int>>(ToJson(new { value = new[]{ 1, 2, 3 } })).Value.SequenceEqual(new[] { 1, 2, 3 }));

		public void ICollection_of_T() => Check.That(
			() => FromJson<ICollection<int>>(ToJson(new { value = new[] { 321 } })).Value.SequenceEqual(new[] { 321 }));

		public void Array_of_T() => Check.That(
			() => FromJson<int[]>(ToJson(new { value = new[] { 321 } })).Value.SequenceEqual(new[] { 321 }));

		public void dispose_enumerators() {
			var items = new MyCollection<string> { "Hello World!" };
			var enumeratorCreated = false;
			var enumeratorDisposed = false;
			items.EnumeratorCreated += xs => {
				enumeratorCreated = true;
				xs.Disposed += (_,__) => enumeratorDisposed = true;
			};

			ToJson(Value(items));
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
		static WithItemOrArrayValue<T> FromJson<T>(string json) => 
			JsonConvert.DeserializeObject<WithItemOrArrayValue<T>>(json);
	}
}

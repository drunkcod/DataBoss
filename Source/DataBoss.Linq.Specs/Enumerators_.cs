using System;
using System.Collections.Generic;
using System.Linq;
using CheckThat;
using Xunit;

namespace DataBoss.Linq
{
	public class Enumerators_
	{
		sealed class Enumerator<T> : IEnumerator<T>
		{
			readonly IEnumerator<T> inner;

			public Enumerator(IEnumerator<T> inner) {
				this.inner = inner;
			}

			public T Current => inner.Current;

			public bool IsDisposed { get; private set; }

			object System.Collections.IEnumerator.Current => ((System.Collections.IEnumerator)inner).Current;

			public void Dispose() {
				IsDisposed = true;
				inner.Dispose();
			}

			public bool MoveNext() =>
				inner.MoveNext();
			
			public void Reset() =>
				inner.Reset();
		}

		class MyEnumerable<T>
		{
			IEnumerator<T> items;
			public MyEnumerable(IEnumerator<T> items) { this.items = items; }

			public IEnumerator<T> GetEnumerator() => items;
		}
		
		[Fact]
		public void @foreach() {
			var xs = GetEnumerator(Items(3));
			foreach(var x in new MyEnumerable<int>(xs))
				;
			Check.That(() => xs.IsDisposed);
		}

		[Fact]
		public void ToList() {
			var items = Items(4);
			var xs = GetEnumerator(items);

			Check.That(
				() => xs.ToList().SequenceEqual(items),
				() => xs.IsDisposed);
		}

		[Fact]
		public void ToArray() {
			var items = Items(17);
			var xs = GetEnumerator(items);

			Check.That(
				() => Items(1).GetEnumerator().ToArray().SequenceEqual(Items(1)),
				() => xs.ToArray().SequenceEqual(items),
				() => xs.IsDisposed);
		}

		[Fact]
		public void Batch() {
			var items = Items(3);
			var xs = GetEnumerator(items);			

			Check.With(() => xs.Batch(2).ToList()).That(
				x => x[0].SequenceEqual(items.Take(2)),
				x => x[1].SequenceEqual(items.Skip(2).Take(2)),
				_ => xs.IsDisposed);
		}

		[Fact]
		public void Batch_MemoryPool() {
			var items = Items(97);
			var memory = new TestMemoryPool<int> { DefaultBufferSize = 10 };
			var batches = items.GetEnumerator().Batch(memory).ToList();

			Check.That(
				() => batches.Count == 10,
				() => batches.Sum(x => x.Memory.Length) == items.Count,
				() => batches.SelectMany(x => x.Memory.ToArray()).SequenceEqual(items));
		}

		[Fact]
		public void Count() {
			var items = Items(5);
			var xs = GetEnumerator(items);

			Check.That(
				() => xs.Count() == items.Count,
				() => xs.IsDisposed);
		}

		[Fact]
		public void First() {
			var items = Enumerable.Range(0, int.MaxValue);
			var xs = GetEnumerator(items);

			Check.That(
				() => xs.First() == items.First(),
				() => xs.IsDisposed);
		}

		[Fact]
		public void First_no_elements_raises_exception() {
			var xs = GetEnumerator(Enumerable.Empty<int>());

			Check.Exception<InvalidOperationException>(() => xs.First());
			Check.That(() => xs.IsDisposed);
		}

		[Fact]
		public void Single() {
			var items = new[]{ "hello" };
			var xs = GetEnumerator(items);

			Check.That(
				() => xs.Single() == items.Single(),
				() => xs.IsDisposed);
		}

		[Fact]
		public void Single_no_elements_raises_exception() {
			var xs = GetEnumerator(Enumerable.Empty<int>());

			Check.Exception<InvalidOperationException>(() => xs.Single());
			Check.That(() => xs.IsDisposed);
		}

		[Fact]
		public void Single_more_than_one_elements_raises_exception() {
			var xs = GetEnumerator(Items(2));

			Check.Exception<InvalidOperationException>(() => xs.Single());
			Check.That(() => xs.IsDisposed);
		}

		static Enumerator<T> GetEnumerator<T>(IEnumerable<T> xs) => new(xs.GetEnumerator());

		static ICollection<int> Items(int count) => Enumerable.Range(0, count).Select(x => x).ToList();
	}
}

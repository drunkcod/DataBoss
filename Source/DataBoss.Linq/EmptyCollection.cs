using System;
using System.Collections;
using System.Collections.Generic;

namespace DataBoss.Linq
{
	public class EmptyCollection<T> : ICollection<T>, IReadOnlyCollection<T>, IEnumerator<T>
	{
		T IEnumerator<T>.Current => throw new InvalidOperationException();
		object IEnumerator.Current => throw new InvalidOperationException();
		bool IEnumerator.MoveNext() => false;
		void IEnumerator.Reset() { }
		void IDisposable.Dispose() { }

		public int Count => 0;
		public bool IsReadOnly => true;

		public void Add(T item) => throw new InvalidOperationException();
		public void Clear() => throw new InvalidOperationException();
		public bool Remove(T item) => throw new InvalidOperationException();

		public bool Contains(T item) => false;
		public void CopyTo(T[] array, int arrayIndex) { }

		public IEnumerator<T> GetEnumerator() => this;
		IEnumerator IEnumerable.GetEnumerator() => this;

		internal EmptyCollection() { }
	}
}
using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;

namespace DataBoss.Data
{
	public class ErrorEnumerable<T> : IEnumerable<T>
	{
		class ErrorEnumerator : IEnumerator<T> {
			ExceptionDispatchInfo error;

			public ErrorEnumerator(ExceptionDispatchInfo error) { this.error = error; }

			public T Current { 
				get {
					error.Throw();
					return default;
				}
			}

			object IEnumerator.Current => Current;

			public void Dispose() { }

			public bool MoveNext() {
				error.Throw();
				return false;
			}

			public void Reset() { }
		}

		readonly ExceptionDispatchInfo error;

		public ErrorEnumerable(Exception ex) { this.error = ExceptionDispatchInfo.Capture(ex); }

		public IEnumerator<T> GetEnumerator() => new ErrorEnumerator(error);

		IEnumerator IEnumerable.GetEnumerator() => new ErrorEnumerator(error);
	}
}

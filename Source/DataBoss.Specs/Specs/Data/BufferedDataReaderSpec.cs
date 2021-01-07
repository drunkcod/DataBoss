using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CheckThat;
using DataBoss.Data;
using Xunit;

namespace DataBoss.Specs.Specs.Data
{
	public class BufferedDataReaderSpec
	{
		class MyThing { public int Value; }

		[Fact]
		public void forwards_read_failure() {
			var message = "Something went kaboom.";
			var e = Check.Exception<InvalidOperationException>(
				() => SequenceDataReader.Create(new ErrorEnumerable<MyThing>(new InvalidOperationException(message))).AsBuffered().Read());
			Check.That(() => e.Message == message);
		}

		class ErrorEnumerable<T> : IEnumerable<T>
		{
			class ErrorEnumerator : IEnumerator<T>
			{
				readonly ErrorEnumerable<T> parent;

				public ErrorEnumerator(ErrorEnumerable<T> parent) { this.parent = parent; }

				public T Current => throw parent.ex;

				object IEnumerator.Current => Current;

				public void Dispose() { }

				public bool MoveNext() {
					throw parent.ex;
				}

				public void Reset() {}
			}

			readonly Exception ex;

			public ErrorEnumerable(Exception ex) { this.ex = ex;  }

			public IEnumerator<T> GetEnumerator() => new ErrorEnumerator(this);

			IEnumerator IEnumerable.GetEnumerator() => new ErrorEnumerator(this);
		}
	}
}

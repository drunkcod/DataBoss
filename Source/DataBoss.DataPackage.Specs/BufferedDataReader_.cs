using System;
using System.Collections.Generic;
using System.Linq;
using CheckThat;
using Xunit;

namespace DataBoss.Data
{
	public class BufferedDataReader_
	{
		class MyThing
		{
			public int Value { get; set; }
		}

		[Fact]
		public void forwards_read_failure() {
			var message = "Something went kaboom.";
			var e = Check.Exception<InvalidOperationException>(
				() => SequenceDataReader.Create(new ErrorEnumerable<MyThing>(new InvalidOperationException(message))).AsBuffered().Read());
			Check.That(() => e.Message == message);
		}

		[Fact]
		public void matches_input() {
			var xs = Enumerable.Range(0, 100);
			var r = SequenceDataReader.Create(xs.Select(x => new MyThing { Value = x })).AsBuffered();

			var ys = new List<int>();
			while (r.Read()) ys.Add(r.GetInt32(0));

			Check.That(() => xs.SequenceEqual(ys));
		}
	}
}

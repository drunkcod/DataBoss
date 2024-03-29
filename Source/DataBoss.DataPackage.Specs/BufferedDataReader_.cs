using System;
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
	}
}

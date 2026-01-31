using System;
using System.Collections.Generic;
using System.Linq;
using CheckThat;
using Xunit;

namespace DataBoss.Data
{
	public class TupleRecordReader_
	{
		class MyThing
		{
			public int Value { get; set; }
		}

		[Fact]
		public void matches_input() {
			var xs = Enumerable.Range(0, 100);
			var r = new TupleRecordReader(SequenceDataReader.Create(xs.Select(x => new MyThing { Value = x })));

			var ys = new List<int>();
			while (r.Read()) ys.Add(r.GetRecord().GetInt32(0));

			Check.That(() => xs.SequenceEqual(ys));
		}

		[Fact]
		public void supports_two_fields() {
			var data = new[] { (Id: 1, Name: "A"), (Id: 2, Name: "B") };
			var reader = SequenceDataReader.Create(data);
			var tr = new TupleRecordReader(reader);

			var result = new List<(int, string)>();
			while (tr.Read()) {
				var r = tr.GetRecord();
				result.Add((r.GetInt32(0), r.GetString(1)));
			}

			Check.That(() => result.Count == 2);
			Check.That(() => result[0].Item1 == 1 && result[0].Item2 == "A");
		}

		[Fact]
		public void supports_eight_fields() {
			var data = new[] { new EightThings { A = 1, B = 2, C = 3, D = 4, E = 5, F = 6, G = 7, H = "8" } };
			var reader = SequenceDataReader.Create(data);
			var tr = new TupleRecordReader(reader);

			Check.That(() => tr.Read());
			var r = tr.GetRecord();
			Check.That(() => r.GetInt32(0) == 1);
			Check.That(() => r.GetInt32(6) == 7);
			Check.That(() => r.GetString(7) == "8");
			Check.That(() => r.FieldCount == 8);
		}

		[Fact]
		public void supports_ten_fields() {
			var data = new[] { new TenThings { A = 1, B = 2, C = 3, D = 4, E = 5, F = 6, G = 7, H = 8, I = 9, J = "10" } };
			var reader = SequenceDataReader.Create(data);
			var tr = new TupleRecordReader(reader);

			Check.That(() => tr.Read());
			var r = tr.GetRecord();
			Check.That(() => r.GetInt32(0) == 1);
			Check.That(() => r.GetInt32(7) == 8);
			Check.That(() => r.GetInt32(8) == 9);
			Check.That(() => r.GetString(9) == "10");
			Check.That(() => r.FieldCount == 10);
		}

		class EightThings
		{
			public int A, B, C, D, E, F, G;
			public string H;
		}

		class TenThings
		{
			public int A, B, C, D, E, F, G, H, I;
			public string J;
		}
	}
}

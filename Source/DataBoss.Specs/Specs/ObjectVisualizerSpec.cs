using System;
using System.IO;
using CheckThat;
using DataBoss.Data;
using Xunit;

namespace DataBoss
{
	public class ObjectVisualizerSpec
	{
		public class ObjectVisualizer_Dump
		{
			StringWriter Output;
			ObjectVisualizer Visualizer;

			public ObjectVisualizer_Dump() {
				Output = new StringWriter();
				Visualizer = new ObjectVisualizer(Output);
			}

			string Dump<T>(T value) {
				Visualizer.Append(value);
				return Output.ToString();
			}

			string Dump<T>(T value, int maxDepth) {
				Visualizer.MaxDepth = maxDepth;
				return Dump(value);
			}

			[Fact]
			public void puts_each_object_on_single_line() {
				Dump("Hello World!");
				Dump(42);
				Check.That(() => Output.ToString() == "Hello World!\r\n42\r\n");
			}

			[Fact]
			public void outputs_row_per_item_in_sequence() {
				Check.That(() => Dump(new[] { 1, 2, 3 }) == "1\r\n2\r\n3\r\n");
			}

			[Fact]
			public void displays_prop_and_value_for_single_object() {
				Check.That(() => Dump(new { Hello = "World" }) == "Hello: World\r\n");
			}

			[Fact]
			public void aligns_members_for_single_object() {
				Check.That(() => Dump(
					new {
						First = "One",
						Two = "Second",
					}) == Lines(
						"First: One",
						"Two  : Second"
					));
			}

			[Fact]
			public void nested_object() {
				Check.That(() => Dump(
					new {
						Values = new[] { 1, 2, 3 },
					}) == Lines(
						"Values: 1",
						"      : 2",
						"      : 3"
					));
			}

			[Fact]
			public void multiline_string() {
				Check.That(() => Dump(
					new {
						Greeting = "Hello\r\nWorld",
					}) == Lines(
						"Greeting: Hello",
						"        : World"
					));
			}

			string Lines(params string[] lines){
				return string.Join("\r\n", lines) + "\r\n";
			}

			[Fact]
			public void uses_prop_name_as_heading_for_sequence() {
				Check.That(() => Dump(
					new[] {
						new { Hello = "Row 1" },
						new { Hello = "Row 2" }
					}) == "Hello\r\nRow 1\r\nRow 2\r\n");
			}

			[Fact]
			public void delimits_columns() {
				Check.That(() => Dump(
					new[] {
						new { Hello = "Bar", World = 1 },
						new { Hello = "Row 2", World = 42 }
					}) == "Hello │ World\r\nBar   │     1\r\nRow 2 │    42\r\n");
			}

			[Fact]
			public void adjusts_column_widhts_to_biggest_row() {
				Check.That(() => Dump(
					new[] {
						new { A = "1", B = 2 },
						new { A= "234", B = 43 }
					}) == "A   │ B\r\n1   │  2\r\n234 │ 43\r\n");
			}

			[Fact]
			public void treats_IDataReader_like_sequence() {
				Dump(SequenceDataReader.Create(new[] {
					new { ABC = "1", B = 2 },
					new { ABC = "23", B = 43 }
				}, "ABC", "B"));
				Check.That(() => Output.ToString() == "ABC │ B\r\n1   │  2\r\n23  │ 43\r\n");
			}

			[Fact]
			public void right_justifies_numbers() {
				Dump(SequenceDataReader.Create(new[] {
					new { ABC = 1 },
				}, "ABC"));
				Check.That(() => Output.ToString() == Lines("ABC","  1"));
			}

			[Fact]
			public void stops_at_max_depth() {
				Check.That(() => Dump(new {
					Top = new {
						Next = new {
							Third = new {
								TooDeep = 42,
							}
						}
					}
				}, 3) == Lines("Top: Next: Third: …"));
			}

			[Fact]
			public void enums() {
				Check.That(() => Dump(FileAccess.Read) == Lines("Read"));
			}

			[Fact]
			public void DateTime_is_displayed_as_string() {
				var now = DateTime.Now;
				Check.That(() => Dump(now) == Lines(now.ToString()));
			}

			[Fact]
			public void nulls_gonna_null() {
				Check.That(() => Dump((string)null) == Lines("null"));
			}

			class MyThing {
				public string MyProp { get; set; }
				public string MyField;
			}

			[Fact]
			public void null_props_are_handled_gracefully() {
				Check.That(() => Dump(new { Foo = (MyThing)null }) == Lines("Foo: null"));
			}

			[Fact]
			public void show_public_fields() {
				Check.That(() => Dump(new MyThing { MyProp = "Prop", MyField = "Field" }) == Lines("MyProp : Prop", "MyField: Field"));
			}

			[Fact]
			public void nested_complex_objects() {
				Check.That(() => Dump(new[] {
					new MyThing { MyProp = "Prop", MyField = null },
					new MyThing { MyProp = null, MyField = "Field" } }) 
				== Lines(
					"MyProp │ MyField", 
					"Prop   │ null",
					"null   │ Field"));
			}
		}
	}
}

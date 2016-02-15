using System.IO;
using System.Text;
using System.Threading.Tasks;
using Cone;
using DataBoss.Data;

namespace DataBoss.Specs
{
	[Describe(typeof(ObjectVisualizer))]
	public class ObjectVisualizerSpec
	{
		[Context("Dump")]
		public class ObjectVisualizer_Dump
		{
			StringWriter Output;

			[BeforeEach]
			public void ResetOutput() {
				Output = new StringWriter();
				ObjectVisualizer.Output = Output;
			}

			public void returns_self() {
				var self = "Hello World";
				Check.That(() => object.ReferenceEquals(self, self.Dump()));
			}

			public void puts_each_object_on_single_line() {
				"Hello World!".Dump();
				42.Dump();
				Check.That(() => Output.ToString() == "Hello World!\r\n42\r\n");
			}

			public void outputs_row_per_item_in_sequence() {
				new[] { 1, 2, 3 }.Dump();
				Check.That(() => Output.ToString() == "1\r\n2\r\n3\r\n");
			}

			public void displays_prop_and_value_for_single_object() {
				new { Hello = "World" }.Dump();
				Check.That(() => Output.ToString() == "Hello: World\r\n");
			}

			public void uses_prop_name_as_heading_for_sequence() {
				new[] {
					new { Hello = "Row 1" },
					new { Hello = "Row 2" }
				}.Dump();
				Check.That(() => Output.ToString() == "Hello\r\nRow 1\r\nRow 2\r\n");
			}

			public void delimits_columns() {
				new[] {
					new { Hello = "Bar", World = 1 },
					new { Hello = "Row 2", World = 42 }
				}.Dump();
				Check.That(() => Output.ToString() == "Hello │ World\r\nBar   │     1\r\nRow 2 │    42\r\n");
			}

			public void adjusts_column_widhts_to_biggest_row() {
				new[] {
					new { A = "1", B = 2 },
					new { A= "234", B = 43 }
				}.Dump();
				Check.That(() => Output.ToString() == "A   │ B\r\n1   │  2\r\n234 │ 43\r\n");
			}

			public void treats_IDataReader_like_sequence() {
				SequenceDataReader.Create(new[] {
					new { ABC = "1", B = 2 },
					new { ABC = "23", B = 43 }
				}, "ABC", "B").Dump();
				Check.That(() => Output.ToString() == "ABC │ B\r\n1   │  2\r\n23  │ 43\r\n");
			}

			public void right_justifies_numbers() {
				SequenceDataReader.Create(new[] {
					new { ABC = 1 },
				}, "ABC").Dump();
				Check.That(() => Output.ToString() == "ABC\r\n  1\r\n");
			}
		}
	}
}

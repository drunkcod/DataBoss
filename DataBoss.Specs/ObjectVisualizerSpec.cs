using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Cone;
using DataBoss.Data;

namespace DataBoss.Specs
{
	class DataGrid
	{
		struct DataGridColumn
		{
			public DataGridColumn(string name, Type type) {
				this.Name = name;
				this.ColumnType = type;
			}

			public readonly string Name;
			public readonly Type ColumnType;
		}
		const string Separator = " | ";

		readonly List<string[]> outputRows = new List<string[]>(); 
		readonly List<DataGridColumn> columns = new List<DataGridColumn>(); 
		readonly List<int> widths = new List<int>();

		public bool ShowHeadings = true;

		public void AddColumn(string name, Type type) {
			columns.Add(new DataGridColumn(name, type));
			widths.Add(name.Length);
		}

		public void AddRow(params object[] row) {
			var output = Array.ConvertAll(row, x => x.ToString());
			for(var i = 0; i != output.Length; ++i)
				widths[i] = Math.Max(widths[i], output[i].Length);
			outputRows.Add(output);
		}

		public void WriteTo(TextWriter output) {
			var values = new string[columns.Count];
			var columnFormat = widths.Select(x => $"{{0,-{x}}}").ToArray();
			Action<string[]> write = row => {
				for(var i = 0; i != row.Length; ++i)
					values[i] = string.Format(columnFormat[i], row[i]);
				output.WriteLine(string.Join(Separator, values).TrimEnd());
			};
			if(ShowHeadings)
				write(columns.Select(x => x.Name).ToArray());
			outputRows.ForEach(write);
		}
	}

	public static class ObjectVisualizer
	{
		public static TextWriter Output = Console.Out;

		public static T Dump<T>(this T self) {
			Dump(self, typeof(T)).WriteTo(Output);
			return self;
		}

		static DataGrid Dump(object obj, Type type) {
			if(IsBasicType(type)) {
				var data = new DataGrid { ShowHeadings = false };
				data.AddColumn(string.Empty, type);
				data.AddRow(obj);
				return data;
			}

			var xs = obj as IEnumerable;
			if(xs != null)
				return DumpSequence(type, xs);

			var reader = obj as IDataReader;
			if(reader != null)
				return DumpReader(reader);
			return DumpObject(obj, type);
		}

		private static DataGrid DumpObject(object obj, Type type)
		{
			var d = new DataGrid {ShowHeadings = false};
			d.AddColumn(string.Empty, typeof(string));
			foreach(var item in type.GetProperties(BindingFlags.Instance | BindingFlags.Public).Where(x => x.CanRead))
				d.AddRow($"{item.Name}: {item.GetValue(obj)}");
			return d;
		}

		private static DataGrid DumpSequence(Type type, IEnumerable xs) {
			var rowType = type.GetInterface(typeof(IEnumerable<>).FullName)?.GenericTypeArguments[0];
			var grid = new DataGrid();

			if(rowType == null || IsBasicType(rowType)) {
				grid.ShowHeadings = false;
				grid.AddColumn(string.Empty, rowType);
				foreach(var item in xs)
					grid.AddRow(item);
				return grid;
			}

			var columns = rowType.GetProperties(BindingFlags.Instance | BindingFlags.Public).Where(x => x.CanRead).ToArray();

			foreach(var item in columns)
				grid.AddColumn(item.Name, item.PropertyType);

			foreach(var item in xs)
				grid.AddRow(Array.ConvertAll(columns, x => x.GetValue(item)));
			return grid;
		}

		private static DataGrid DumpReader(IDataReader reader) {
			var data = new DataGrid();
			for(var i = 0; i != reader.FieldCount; ++i)
				data.AddColumn(reader.GetName(i), reader.GetFieldType(i));
			for(var row = new object[reader.FieldCount]; reader.Read();) {
				reader.GetValues(row);
				data.AddRow(row);
			}
			return data;
		}

		static bool IsBasicType(Type type) {
			return type.IsPrimitive || type == typeof(string);
		}
	}

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
				Check.That(() => Output.ToString() == "Hello | World\r\nBar   | 1\r\nRow 2 | 42\r\n");
			}

			public void adjusts_column_widhts_to_biggest_row() {
				new[] {
					new { A = "1", B = 2 },
					new { A= "234", B = 43 }
				}.Dump();
				Check.That(() => Output.ToString() == "A   | B\r\n1   | 2\r\n234 | 43\r\n");
			}

			public void treats_IDataReader_like_sequence() {
				SequenceDataReader.Create(new[] {
					new { A = "1", B = 2 },
					new { A= "234", B = 43 }
				}, "A", "B").Dump();
				Check.That(() => Output.ToString() == "A   | B\r\n1   | 2\r\n234 | 43\r\n");

			}
		}
	}
}

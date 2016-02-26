using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DataBoss
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

		struct GridValue
		{
			public string[] Lines;
			public int Width;
			public int Height => Lines.Length;
		}

		public string Separator = " │ ";

		readonly List<GridValue[]> outputRows = new List<GridValue[]>(); 
		readonly List<DataGridColumn> columns = new List<DataGridColumn>(); 
		readonly List<int> widths = new List<int>();
		readonly List<int> heights = new List<int>();

		public bool ShowHeadings = true;

		public void AddColumn(string name, Type type) {
			columns.Add(new DataGridColumn(name, type));
			widths.Add(ShowHeadings ? name.Length : 0);
		}

		public void AddRow(params object[] row) {
			var output = Array.ConvertAll(row, ToGridValue);
			var height = 1;
			for(var i = 0; i != output.Length; ++i) {
				widths[i] = Math.Max(widths[i], output[i].Width);
				height = Math.Max(height, output[i].Height);
			}
			outputRows.Add(output);
			heights.Add(height);

		}

		GridValue ToGridValue(object obj) {
			var s = obj.ToString().TrimEnd();
			return new GridValue {
				Lines = s.Split(new[] { "\r\n" }, StringSplitOptions.None),
				Width = s.Length,
			};
		}

		public void WriteTo(TextWriter output) {
			var values = new string[columns.Count];
			var columnFormat = new string[columns.Count];
			for(var i = 0; i != columns.Count; ++i) {
				var n = IsNumberColumn(i) ? widths[i] : -widths[i];
				columnFormat[i] = $"{{0,{n}}}";
			}

			if(ShowHeadings) {
				for(var i = 0; i != columns.Count; ++i)
					values[i] = string.Format($"{{0,-{widths[i]}}}", columns[i].Name);
				output.WriteLine(string.Join(Separator, values).TrimEnd());
			}

			var rowIndex = 0;
			outputRows.ForEach(row => {
				for(var r = 0; r != heights[rowIndex]; ++r) {
					for(var i = 0; i != row.Length; ++i)
						values[i] = string.Format(columnFormat[i], r < row[i].Height ? row[i].Lines[r] : string.Empty);
					output.WriteLine(string.Join(Separator, values).TrimEnd());
				};
				++rowIndex;
			});
		}

		bool IsNumberColumn(int i)  => columns[i].ColumnType == typeof(int);

		public override string ToString()
		{
			var result = new StringWriter();
			WriteTo(result);
			return result.ToString();
		}
	}
}
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

		public const string Separator = " │ ";

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
}
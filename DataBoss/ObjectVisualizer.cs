using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;

namespace DataBoss
{
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
}
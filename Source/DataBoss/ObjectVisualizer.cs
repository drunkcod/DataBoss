using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;

namespace DataBoss.Util
{
	public static class ObjectExtensions
	{
		static TextWriter output = Console.Out;
		static ObjectVisualizer visualizer = new ObjectVisualizer(output);

		public static TextWriter Output
		{
			get { return output; }
			set {
				output = value;
				visualizer = new ObjectVisualizer(value);
			}
		}

		public static T Dump<T>(this T self) {
			visualizer.Append(self);
			return self;
		}
	}
}

namespace DataBoss
{
	public class ObjectVisualizer
	{
		class ColumnInfo
		{
			public string Name;
			public Type ColumnType;
			public Func<object,object> GetValue; 
		}

		static readonly DataGrid MaxDepthReached = DataGrid("…", typeof(string));
		static readonly Dictionary<Type,Func<object,object>> TypeTransforms = new Dictionary<Type, Func<object, object>> {
			{ typeof(DateTime), x => ((DateTime)x).ToString() }
		};

		readonly TextWriter output;

		public int MaxDepth = 3;

		public ObjectVisualizer(TextWriter output) {
			this.output = output;
		}

		public void Append<T>(T self) {
			ToDataGrid(self, typeof(T), MaxDepth).WriteTo(output);
		}

		static DataGrid ToDataGrid(object obj, Type type, int depth) {
			Func<object,object> transform;
			if(TypeTransforms.TryGetValue(type, out transform)) {
				var xformed = transform(obj);
				return ToDataGrid(xformed, xformed.GetType(), depth);
			}

			if(IsBasicType(type)) {
				return DataGrid(obj, type);
			}

			if(depth == 0)
				return MaxDepthReached;

			var reader = obj as IDataReader;
			if (reader != null)
				return DumpReader(reader);

			var xs = obj as IEnumerable;
			if(xs != null)
				return DumpSequence(type, xs);

			return DumpObject(obj, type, depth);
		}

		private static DataGrid DataGrid(object obj, Type type)
		{
			var data = new DataGrid {ShowHeadings = false};
			data.AddColumn("Value", type);
			data.AddRow(obj);
			return data;
		}

		static DataGrid DumpObject(object obj, Type type, int depth) {
			if(obj == null)
				return DataGrid(null, type);
			var d = new DataGrid { ShowHeadings = false, Separator = ": "};
			d.AddColumn("Member", typeof(string));
			d.AddColumn("Value", typeof(string));
			foreach(var item in GetColumns(type))
				d.AddRow(item.Name, ToDataGrid(item.GetValue(obj), item.ColumnType, depth - 1));
			return d;
		}

		static DataGrid DumpSequence(Type type, IEnumerable xs) {
				var rowType = type.GetInterface(typeof(IEnumerable<>).FullName)?.GenericTypeArguments[0];
			var grid = new DataGrid();

			if(rowType == null || IsBasicType(rowType)) {
				grid.ShowHeadings = false;
				grid.AddColumn(string.Empty, rowType);
				foreach(var item in xs)
					grid.AddRow(item);
				return grid;
			}

			var columns = GetColumns(rowType).ToArray();
			foreach(var item in columns)
				grid.AddColumn(item.Name, item.ColumnType);

			foreach(var item in xs)
				grid.AddRow(Array.ConvertAll(columns, x => x.GetValue(item)));
			return grid;
		}

		private static IEnumerable<ColumnInfo> GetColumns(Type rowType)
		{
			return
				rowType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
					.Where(x => x.CanRead)
					.Select(x => new ColumnInfo {Name = x.Name, ColumnType = x.PropertyType, GetValue = x.GetValue})
					.Concat(
						rowType.GetFields(BindingFlags.Instance | BindingFlags.Public)
							.Select(x => new ColumnInfo {Name = x.Name, ColumnType = x.FieldType, GetValue = x.GetValue})
					);
		}

		static DataGrid DumpReader(IDataReader reader) {
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
			return type.IsPrimitive || type.IsEnum
			|| type == typeof(string);
		}
	}
}
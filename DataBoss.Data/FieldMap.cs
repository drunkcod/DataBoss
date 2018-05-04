using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace DataBoss.Data
{
	public struct FieldMapItem
	{
		public readonly int Ordinal;
		public readonly Type FieldType;
		public readonly bool AllowDBNull;

		public FieldMapItem(int ordinal, Type fieldType, bool allowDBNull) {
			this.Ordinal = ordinal;
			this.FieldType = fieldType;
			this.AllowDBNull = allowDBNull;
		}

		public override string ToString() => $"({Ordinal}, {FieldType.FullName})";
	}

	public class FieldMap
	{
		readonly Dictionary<string, FieldMapItem> fields = new Dictionary<string, FieldMapItem>();
		Dictionary<string, FieldMap> subFields;

		public static FieldMap Create(IDataReader reader) {
			var fieldMap = new FieldMap();
			var schema = reader.GetSchemaTable();
			var ordinalColumn = schema.Columns[DataReaderSchemaColumns.ColumnOrdinal];
			var allowDBNullColumn = schema.Columns[DataReaderSchemaColumns.AllowDBNull];
			for(var i = 0; i != reader.FieldCount; ++i) { 
				var isNullable = 
					ordinalColumn != null 
					&& allowDBNullColumn != null 
					&& (bool)schema.Rows.Cast<DataRow>().Single(x => (int)x[ordinalColumn] == i)[allowDBNullColumn];
				fieldMap.Add(reader.GetName(i), i, reader.GetFieldType(i), isNullable);
			}
			return fieldMap;
		}

		public int MinOrdinal => fields.Count == 0 ? -1 : fields.Min(x => x.Value.Ordinal);

		public void Add(string name, int ordinal, Type fieldType, bool allowDBNull) {
			if(name.Contains('.')) {
				var parts = name.Split('.');
				var x = this;
				for(var n = 0; n != parts.Length - 1; ++n)
					x = x[parts[n]];
				x.Add(parts[parts.Length - 1], ordinal, fieldType, allowDBNull);
			}
			fields.Add(name, new FieldMapItem(ordinal, fieldType, allowDBNull));
		}

		public bool TryGetOrdinal(string key, out FieldMapItem item) =>
			fields.TryGetValue(key, out item);

		public bool TryGetSubMap(string key, out FieldMap subMap) {
			if(subFields != null && subFields.TryGetValue(key, out subMap))
				return true;
			subMap = null;
			return false;
		}

		public override string ToString() => string.Join(", ", fields.OrderBy(x => x.Value.Ordinal).Select(x => $"{x.Value.FieldType} [{x.Key}]"));

		FieldMap this[string name] {
			get {
				if(subFields == null)
					subFields = new Dictionary<string, FieldMap>();
				return subFields.GetOrAdd(name, _ => new FieldMap());
			}
		}
	}
}
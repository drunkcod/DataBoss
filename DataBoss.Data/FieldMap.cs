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

		public FieldMapItem(int ordinal, Type fieldType) {
			this.Ordinal = ordinal;
			this.FieldType = fieldType;
		}

		public override string ToString() => $"({Ordinal}, {FieldType.FullName})";
	}

	public class FieldMap
	{
		readonly Dictionary<string, FieldMapItem> fields = new Dictionary<string, FieldMapItem>();
		Dictionary<string, FieldMap> subFields;

		public static FieldMap Create(IDataRecord reader) {
			var fieldMap = new FieldMap();
			for(var i = 0; i != reader.FieldCount; ++i)
				fieldMap.Add(reader.GetName(i), i, reader.GetFieldType(i));
			return fieldMap;
		}

		public int MinOrdinal => fields.Count == 0 ? -1 : fields.Min(x => x.Value.Ordinal);

		public void Add(string name, int ordinal, Type fieldType) {
			if(name.Contains('.')) {
				var parts = name.Split('.');
				var x = this;
				for(var n = 0; n != parts.Length - 1; ++n)
					x = x[parts[n]];
				x.Add(parts[parts.Length - 1], ordinal, fieldType);
			}
			fields.Add(name, new FieldMapItem(ordinal, fieldType));
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
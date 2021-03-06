using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

namespace DataBoss.Data
{
	public class FieldMap : IEnumerable<(string FieldName, FieldMapItem Item)>
	{
		readonly Dictionary<string, FieldMapItem> fields = new Dictionary<string, FieldMapItem>(StringComparer.InvariantCultureIgnoreCase);
		Dictionary<string, FieldMap> subFields;

		public static FieldMap Create(IDataReader reader) => Create(reader, _ => true);
		public static FieldMap Create(IDataReader reader, Predicate<DataReaderSchemaRow> include) {
			var fieldMap = new FieldMap();
			var schema = reader.GetSchemaTable();
			var ordinalColumn = schema.Columns[DataReaderSchemaColumns.ColumnOrdinal.Name];
			var allowDBNullColumn = schema.Columns[DataReaderSchemaColumns.AllowDBNull.Name];
			var getProviderSpecificFieldType = GetGetProviderSpecificFieldType(reader);
			for(var i = 0; i != reader.FieldCount; ++i) {
				var item = new DataReaderSchemaRow {
					ColumnName = reader.GetName(i),
					Ordinal = i,
					ColumnType = reader.GetFieldType(i),
					ProviderSpecificDataType = getProviderSpecificFieldType(i),
					AllowDBNull = ordinalColumn != null
						&& allowDBNullColumn != null
						&& (bool)schema.Rows.Cast<DataRow>().Single(x => (int)x[ordinalColumn] == i)[allowDBNullColumn]	
				};
				if(include(item))
					fieldMap.Add(item.ColumnName, item.Ordinal, item.ColumnType, item.ProviderSpecificDataType, item.AllowDBNull);
			}
			return fieldMap;
		}

		static Func<int, Type> GetGetProviderSpecificFieldType(IDataReader reader) {
			var getter = reader.GetType().GetMethod(nameof(DbDataReader.GetProviderSpecificFieldType), new[] { typeof(int) });
			if(getter == null)
				return _ => null;
			return (Func<int, Type>)Delegate.CreateDelegate(typeof(Func<int, Type>), reader, getter);
		}

		public int Count => fields.Count;
		public int MinOrdinal => fields.Count == 0 ? -1 : fields.Min(x => x.Value.Ordinal);

		public void Add(string name, int ordinal, Type fieldType, Type providerSpecificFieldType, bool allowDBNull) {
			if(name.Contains('.')) {
				var parts = name.Split('.');
				var x = this;
				for(var n = 0; n != parts.Length - 1; ++n)
					x = x[parts[n]];
				x.Add(parts[parts.Length - 1], ordinal, fieldType, providerSpecificFieldType, allowDBNull);
			}
			fields.Add(name, new FieldMapItem(ordinal, fieldType, providerSpecificFieldType, allowDBNull));
		}

		public bool TryGetField(string key, out FieldMapItem item) =>
			fields.TryGetValue(key, out item);

		public bool TryGetSubMap(string key, out FieldMap subMap) {
			if(subFields != null && subFields.TryGetValue(key, out subMap))
				return true;
			subMap = null;
			return false;
		}

		public override string ToString() => string.Join(", ", fields.OrderBy(x => x.Value.Ordinal).Select(x => $"{x.Value.FieldType} [{x.Key}]"));

		public IEnumerator<(string, FieldMapItem)> GetEnumerator() {
			foreach (var item in fields)
				yield return (item.Key, item.Value);
		}

		IEnumerator IEnumerable.GetEnumerator() =>
			GetEnumerator();

		FieldMap this[string name] {
			get {
				if(subFields == null)
					subFields = new Dictionary<string, FieldMap>();
				return subFields.GetOrAdd(name, _ => new FieldMap());
			}
		}
	}
}
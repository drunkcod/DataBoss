using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace DataBoss.Data
{
	public abstract class DataSeries
	{
		readonly BitArray isNull;

		public DataSeries(Type type, bool allowNulls) {
			this.Type = type;
			this.isNull = allowNulls ? new BitArray(0) : null;
		}

		public bool AllowNulls => isNull != null;
		public abstract int Count { get; }
		public abstract string Name { get; }

		internal void ReadItem(IDataRecord record, int ordinal) {
			if (AllowNulls) {

				var isNullItem = record.IsDBNull(ordinal);
				isNull.Length = Count + 1;
				isNull[Count] = isNullItem;

				if (isNullItem) {
					AddDefault();
					return;
				}
			}
			AddItem(record, ordinal);
		}

		protected virtual void AddDefault() { }
		protected virtual void AddItem(IDataRecord record, int ordinal) { }

		public Type Type { get; }
		public bool IsNull(int index) => AllowNulls && isNull[index];
		public abstract T GetValue<T>(int index);
	}

	public class DataSeries<TItem> : DataSeries
	{
		readonly List<TItem> items = new();

		public DataSeries(string name, bool allowNulls) : base(typeof(TItem), allowNulls) {
			this.Name = name;
		}

		public override int Count => items.Count;
		public override string Name { get; }
		public TItem this[int index] => items[index];

		public override T GetValue<T>(int index) =>
			(T)(object)items[index];

		protected override void AddDefault() =>
			items.Add(default);

		protected override void AddItem(IDataRecord record, int ordinal) =>
			items.Add(record.GetFieldValue<TItem>(ordinal));

	}

}

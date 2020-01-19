using System;

namespace DataBoss.Data
{
	public struct FieldMapItem
	{
		public readonly int Ordinal;
		public readonly Type FieldType;
		public readonly Type ProviderSpecificFieldType;
		public readonly bool CanBeNull;

		public FieldMapItem(int ordinal, Type fieldType, Type providerSpecificFieldType, bool allowDBNull) {
			this.Ordinal = ordinal;
			this.FieldType = fieldType;
			this.ProviderSpecificFieldType = providerSpecificFieldType;
			this.CanBeNull = allowDBNull;
		}

		public override string ToString() => $"({Ordinal}, {FieldType.FullName})";
	}
}
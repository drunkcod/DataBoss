using System;
using System.Data.SqlTypes;
using System.Net;

namespace DataBoss.Data.SqlServer
{
	public struct RowVersion : IComparable<RowVersion>
	{
		public readonly SqlBinary Value;
		
		public RowVersion(SqlBinary value) {
			if(value == null || value.Length != 8)
				throw new InvalidOperationException("RowVersion must be 8 bytes");
			this.Value = value; 
		}

		[ConsiderAsCtor]
		public static RowVersion From(long value) => new RowVersion(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value)));

		[ConsiderAsCtor]
		public static RowVersion From(byte[] value) => new RowVersion(new SqlBinary(value));

		public long ToInt64() => IPAddress.NetworkToHostOrder(BitConverter.ToInt64(Value.Value, 0));

		public int CompareTo(RowVersion other) => Value.CompareTo(other.Value);

		public override string ToString() => ToInt64().ToString();
		public override bool Equals(object obj) => CompareTo(((RowVersion)obj)) == 0;
		public override int GetHashCode() => ToInt64().GetHashCode();

		public static explicit operator long(RowVersion self) => self.ToInt64();
		public static explicit operator byte[](RowVersion self) => self.Value.Value;

		public static bool operator==(RowVersion x, RowVersion y) => x.CompareTo(y) == 0;
		public static bool operator!=(RowVersion x, RowVersion y) => x.CompareTo(y) != 0;
	}
}

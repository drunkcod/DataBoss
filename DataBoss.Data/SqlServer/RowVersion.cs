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

		public static RowVersion From(long value) => new RowVersion(new SqlBinary(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(value))));

		public long ToInt64() => IPAddress.NetworkToHostOrder(BitConverter.ToInt64(Value.Value, 0));

		public int CompareTo(RowVersion other) => Value.CompareTo(other.Value);

		public override string ToString() => ToInt64().ToString();
	}
}

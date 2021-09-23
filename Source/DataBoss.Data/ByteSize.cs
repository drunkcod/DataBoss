namespace DataBoss.Data
{
	public struct ByteSize
	{
		static readonly string[] Unit = new[] { "KiB", "MiB", "GiB", "TiB", "PiB" };
		static readonly long[] Threshold = new[] { 1L << 10, 1L << 20, 1L << 30, 1L << 40, 1L << 50 };
		
		public readonly long TotalBytes;

		public ByteSize(long totalBytes) { this.TotalBytes = totalBytes; }

		public static implicit operator long(ByteSize size) => size.TotalBytes;

		public override string ToString() {
			var unit = 0;
			for (; unit < Threshold.Length && Threshold[unit] < TotalBytes; ++unit)
				;

			if (unit == 0)
				return TotalBytes.ToString();
			unit -= 1;
			return string.Format("{0:N2} {1}", (1.0 * TotalBytes) / Threshold[unit], Unit[unit]);
		}
	}
}

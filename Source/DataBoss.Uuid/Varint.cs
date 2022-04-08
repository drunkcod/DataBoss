using System;

namespace DataBoss.Encoding
{
	public static class Varint
	{
		public const int MaxSize = 10;

		public static byte[] GetBytes(long x) => GetBytes(ZigZag(x));

		public static byte[] GetBytes(ulong ux) {
			var tmp = new byte[MaxSize];
			Array.Resize(ref tmp, GetBytes(ux, tmp));
			return tmp;
		}

		public static int GetBytes(long x, Span<byte> dst) => GetBytes(ZigZag(x), dst);

		public static int GetBytes(ulong ux, Span<byte> dst) {
			var n = 0;
			while (ux >= 0x80) {
				dst[n++] = (byte)(ux | 0x80);
				ux >>= 7;
			}
			dst[n] = (byte)ux;
			return n + 1;
		}

		static ulong ZigZag(long x) {
			var ux = ((ulong)x) << 1;
			return x < 0 ? ~ux : ux;
		}
	}
}

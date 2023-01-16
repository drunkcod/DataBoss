using System;

namespace DataBoss
{
	public static class Varint
	{
		public const int MaxSize = 10;

		public static byte[] GetBytes(long x) => GetBytes(ZigZag(x));

		public static byte[] GetBytes(ulong ux) {
			var tmp = new byte[MaxSize];
			TryGetBytes(ux, tmp, out var n);
			Array.Resize(ref tmp, n);
			return tmp;
		}

		public static bool TryGetBytes(long x, Span<byte> dst, out int bytesWritten) => 
			TryGetBytes(ZigZag(x), dst, out bytesWritten);

		public static bool TryGetBytes(ulong ux, Span<byte> dst, out int bytesWritten) {
			var c = GetByteCount(ux);
			if (dst.Length < c) {
				bytesWritten = 0;
				return false;
			}

			var lastByte = c - 1;
			for (var n = 0; n != lastByte; ++n) {
				dst[n] = (byte)(ux | 0x80);
				ux >>= 7;
			}
			dst[lastByte] = (byte)ux;

			bytesWritten = c;
			return true;
		}

		public static int GetByteCount(ulong x) => 1 + (64 - (int)ulong.LeadingZeroCount(x)) / 7;

		public static ulong ZigZag(long x) => (ulong)((x >> 63) ^ (x << 1));
		public static long UnZigZag(ulong x) => (long)(x >>> 1) ^ -(long)(x & 1);
	}
}

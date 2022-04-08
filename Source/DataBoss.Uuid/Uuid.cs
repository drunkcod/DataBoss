using System;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Security.Cryptography;

namespace DataBoss
{
	public static class Uuid
	{
		public static Guid NewSha1(Guid space, ReadOnlySpan<byte> data) =>
			NewHash("SHA1", 5, space, data);

		static Guid NewHash(string hashName, byte version, Guid space, ReadOnlySpan<byte> data) {
			var r = new byte[16 + data.Length];
			using var h = HashAlgorithm.Create(hashName);

			space.TryWriteBytes(r);
			ToUuidOrder(r);

			data.CopyTo(r.AsSpan()[16..]);

			var uuid = new Span<byte>(h.ComputeHash(r), 0, 16);
			uuid[6] = (byte)((uuid[6] & 0x0f) | (version << 4));
			uuid[8] = (byte)((uuid[8] & 0x3f) | 0x80);
			ToUuidOrder(uuid);
			return new Guid(uuid);
		}

		static void ToUuidOrder(Span<byte> bs) {
			var a = MemoryMarshal.Cast<byte, int>(bs);
			a[0] = HostToNetworkOrder(a[0]);
			var b = MemoryMarshal.Cast<byte, short>(bs.Slice(4));
			b[0] = HostToNetworkOrder(b[0]);
			b[1] = HostToNetworkOrder(b[1]);
		}

		static int HostToNetworkOrder(int host) =>
			BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(host) : host;

		static short HostToNetworkOrder(short host) =>
			BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(host) : host;
	}
}

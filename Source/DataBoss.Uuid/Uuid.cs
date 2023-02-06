using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;

namespace DataBoss
{
	public static class Uuid
	{
		public static readonly Guid Dns = new("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
		public static readonly Guid Url = new("6ba7b811-9dad-11d1-80b4-00c04fd430c8");
		public static readonly Guid Oid = new("6ba7b812-9dad-11d1-80b4-00c04fd430c8");
		public static readonly Guid X500 = new("6ba7b814-9dad-11d1-80b4-00c04fd430c8");

		public static Encoding Encoding = new UTF8Encoding(false);

		public static Guid NewMd5(Guid space, string data) =>
			NewMd5(space, Encoding.GetBytes(data));

		public static Guid NewMd5(Guid space, ReadOnlySpan<byte> data) {
			using var h = MD5.Create();
			return NewHash(h, 3, space, data);
		}

		public static Guid NewSha1(Guid space, string data) =>
			NewSha1(space, Encoding.GetBytes(data));

		public static Guid NewSha1(Guid space, ReadOnlySpan<byte> data) { 
			using var h = SHA1.Create();
			return NewHash(h, 5, space, data);
		}

		static Guid NewHash(HashAlgorithm h, byte version, Guid space, ReadOnlySpan<byte> data) {
			Span<byte> r = stackalloc byte[16 + data.Length];
			Span<byte> uuid = stackalloc byte[h.HashSize / 8];
			space.TryWriteBytes(r);
			ToUuidOrder(r);

			data.CopyTo(r[16..]);

			if(!h.TryComputeHash(r, uuid, out var _))
				throw new Exception($"This should never happen, {h} failed to compute hash of size {uuid.Length} bytes." );
			
			uuid[6] = (byte)((uuid[6] & 0x0f) | (version << 4));
			uuid[8] = (byte)((uuid[8] & 0x3f) | 0x80);
			return Parse(uuid[..16]);
		}

		public static Guid Parse(byte[] bs) => Parse(new ReadOnlySpan<byte>(bs));

		public static Guid Parse(ReadOnlySpan<byte> bs) {
			Span<byte> r = stackalloc byte[16];
			ref GuidPreamble g = ref MemoryMarshal.AsRef<GuidPreamble>(r);
			r[15] = bs[15];
			g.A = BinaryPrimitives.ReadUInt32BigEndian(bs);
			g.B = BinaryPrimitives.ReadUInt16BigEndian(bs[4..]);
			g.C = BinaryPrimitives.ReadUInt16BigEndian(bs[6..]);
			r[8] = bs[8];
			r[9] = bs[9];
			r[10] = bs[10];
			r[11] = bs[11];
			r[12] = bs[12];
			r[13] = bs[13];
			r[14] = bs[14];
			return new Guid(r);
		}

		[StructLayout(LayoutKind.Sequential)]
		struct GuidPreamble 
		{
			public uint A;
			public ushort B;
			public ushort C;
		}

		static void ToUuidOrder(Span<byte> bs) {
			ref var x = ref MemoryMarshal.AsRef<GuidPreamble>(bs[..8]);
			x.A = BinaryPrimitives.ReverseEndianness(x.A);
			x.B = BinaryPrimitives.ReverseEndianness(x.B);
			x.C = BinaryPrimitives.ReverseEndianness(x.C);
		}
	}
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using CheckThat;
using DataBoss.Buffers;
using Xunit;

namespace DataBoss.IO
{
	public class ProducerStream_
	{
		[Fact]
		public void ignores_empty_writes() {
			using var ps = new ProducerStream(NullArrayPool<byte>.Instance, chunkSize: 1024);

			WriteBuffers(ps, 
				new byte[0],
				GetBytes("Hello World"),
				new byte[0],
				GetBytes("."));
			ps.Close();

			var buff = new byte[1024];
			var ns = new List<int>();
			using(var r = ps.OpenConsumer())
				for(var n = 0; ;) {
					n = r.Read(buff, 0, buff.Length);
					if (n == 0)
						break;
					ns.Add(n);				
				}

			Check.That(
				() => !ns.Any(x => x == 0),
				() => ns.Sum() == GetBytes("Hello World.").Length);
		}

		[Fact]
		public void read_merges_available_chunks() {
			using var ps = new ProducerStream(NullArrayPool<byte>.Instance, chunkSize: 1);

			WriteBuffers(ps,
				GetBytes("1"),
				GetBytes("2"),
				GetBytes("3"));

			var buff = new byte[2];
			using var r = ps.OpenConsumer();
			Check.That(() => r.Read(buff, 0, 2) == 2);
		}

		[Fact]
		public void read_continous_parts() {
			using var ps = new ProducerStream(NullArrayPool<byte>.Instance, chunkSize: 1024);
			var source = GetBytes("123");
			WriteBuffers(ps, source);
			ps.Close();

			var buff = new byte[source.Length];
			using var r = ps.OpenConsumer();
			for (var i = 0; i != buff.Length; ++i)
				r.Read(buff, i, 1);
			Check.That(() => buff.SequenceEqual(source));
		}

		byte[] GetBytes(string s) => Encoding.UTF8.GetBytes(s);
		static void WriteBuffers(Stream stream, params byte[][] bs) {
			foreach (var item in bs)
				stream.Write(item, 0, item.Length);
		}
	}
}

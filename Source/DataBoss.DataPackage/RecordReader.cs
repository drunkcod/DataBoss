using System;
using System.Buffers;
using System.Data;
using System.Threading;
using System.Threading.Channels;
using DataBoss.Data;
using DataBoss.Threading;
using DataBoss.Threading.Channels;

namespace DataBoss.DataPackage
{
	class RecordReader(IDataRecordReader reader, ChannelWriter<(IMemoryOwner<IDataRecord2>, int)> writer, CancellationToken cancellation) : WorkItem
	{
		public const int BufferRows = 256;

		readonly IDataRecordReader reader = reader;
		readonly ChannelWriter<(IMemoryOwner<IDataRecord2>, int)> writer = writer;
		readonly CancellationToken cancellation = cancellation;

		protected override void DoWork() {
			var buffer = CreateBuffer();
			var rows = buffer.Memory.Span;
			var n = 0;

			while (!cancellation.IsCancellationRequested && reader.Read()) {
				rows[n] = reader.GetRecord();

				if (++n == BufferRows) {
					writer.Write((buffer, n), cancellation);
					n = 0;
					buffer = CreateBuffer();
					rows = buffer.Memory.Span;
				}
			}

			if (n != 0)
				writer.Write((buffer, n), cancellation);
		}

		static IMemoryOwner<IDataRecord2> CreateBuffer() => MemoryPool<IDataRecord2>.Shared.Rent(BufferRows);

		protected override void Cleanup() =>
			writer.Complete();
	}

}

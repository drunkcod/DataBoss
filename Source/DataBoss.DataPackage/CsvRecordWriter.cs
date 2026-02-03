using System;
using System.Buffers;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;
using DataBoss.Data;
using DataBoss.IO;
using DataBoss.Threading;
using DataBoss.Threading.Channels;

namespace DataBoss.DataPackage;

public class CsvRecordWriter(string delimiter, Encoding encoding)
{
	readonly string delimiter = delimiter;
	public readonly Encoding Encoding = encoding;

	public CsvFragmentWriter NewFragmentWriter(Stream stream) =>
		NewFragmentWriter(NewWriter(stream));

	public CsvFragmentWriter NewFragmentWriter(TextWriter writer) =>
		new(new CsvWriter(writer, delimiter, leaveOpen: true));

	public void WriteHeaderRecord(Stream stream, IDataRecord data) =>
		WriteHeaderRecord(NewWriter(stream), data);

	StreamWriter NewWriter(Stream stream) =>
		new(stream, Encoding, DataPackage.StreamBufferSize, leaveOpen: true);

	public void WriteHeaderRecord(TextWriter writer, IDataRecord data) {
		using var csv = NewFragmentWriter(writer);
		for (var i = 0; i != data.FieldCount; ++i)
			csv.WriteField(data.GetName(i));
		csv.NextRecord();
		csv.Flush();
	}

	public void WriteHeaderRecord(TextWriter writer, in DataReaderStringView view) {
		using var csv = NewFragmentWriter(writer);
		for (var i = 0; i != view.FieldCount; ++i)
			csv.WriteField(view.GetName(i));
		csv.NextRecord();
		csv.Flush();
	}

	public void WriteRecords(TextWriter writer, in DataReaderStringView view) {
		using var csv = NewFragmentWriter(writer);
		while (view.Read())
			csv.WriteRecord(view);
	}

	public Task WriteChunksAsync(ChannelReader<(IMemoryOwner<IDataRecord2>, int)> records, ChannelWriter<Stream> chunks, in DataReaderStringView view) {
		var writer = new ChunkWriter(records, chunks, this) {
			Format = view.Format(),
		};
		return writer.RunAsync();
	}

	public class CsvFragmentWriter(CsvWriter csv) : IDisposable
	{
		readonly CsvWriter csv = csv;

		public void Dispose() => csv.Dispose();

		public void WriteField(string value) => csv.WriteField(value);
		public void NextField() => csv.NextField();

		public void WriteRecord<T>(in T view) where T : IStringRecord {
			var i = 0;
			try {
				for (; i != view.FieldCount; ++i) {
					if (view.IsDBNull(i))
						NextField();
					else
						WriteField(view.GetString(i));
				}
				NextRecord();
			}
			catch (Exception ex) {
				throw new Exception($"Failed writing {view.GetName(i)} with value {view.GetValue(i)}", ex);
			}
		}
		public void NextRecord() => csv.NextRecord();
		public void Flush() => csv.Writer.Flush();
	}

	class ChunkWriter(ChannelReader<(IMemoryOwner<IDataRecord2>, int)> records, ChannelWriter<Stream> chunks, CsvRecordWriter csv) : WorkItem
	{
		readonly ChannelReader<(IMemoryOwner<IDataRecord2> Rows, int Count)> records = records;
		readonly ChannelWriter<Stream> chunks = chunks;
		readonly CsvRecordWriter csv = csv;
		int chunkCapacity = DataPackage.StreamBufferSize;
		public RecordStringViewFormat Format;
		public int MaxWorkers = 1;

		protected override void DoWork() {
			if (MaxWorkers == 1)
				WriteAllRecords();
			else
				records.GetConsumingEnumerable().AsParallel()
					.WithDegreeOfParallelism(MaxWorkers)
					.ForAll(WriteRecords);
		}

		void WriteAllRecords() {
			using var ps = new ProducerStream();
			chunks.Write(ps.OpenConsumer());
			using var result = csv.NewFragmentWriter(ps);
			var view = Format.NewItemView();
			do {
				while (records.TryRead(out var item))
					try {
						foreach (var r in item.Rows.Memory[..item.Count].Span) {
							view.Current = r;
							result.WriteRecord(view);
							r.Dispose();
						}
					}
					finally {
						item.Rows.Dispose();
					}
			} while (records.WaitToRead());
		}

		void WriteRecords((IMemoryOwner<IDataRecord2> Rows, int Count) item) {
			try {
				var rows = item.Rows.Memory[..item.Count].Span;
				if (rows.Length == 0)
					return;

				var chunk = new MemoryStream(chunkCapacity);
				var view = Format.NewItemView();
				using (var fragment = csv.NewFragmentWriter(chunk))
					foreach (var r in rows) {
						view.Current = r;
						fragment.WriteRecord(view);
						r.Dispose();

					}
				if (chunk.Position != 0) {
					chunkCapacity = Math.Max(chunkCapacity, chunk.Capacity);
					WriteChunk(chunk);
				}
			}
			finally {
				item.Rows.Dispose();
			}
		}

		void WriteChunk(MemoryStream chunk) {
			chunk.Position = 0;
			chunks.Write(chunk);
		}

		protected override void Cleanup() =>
			chunks.Complete();
	}
}

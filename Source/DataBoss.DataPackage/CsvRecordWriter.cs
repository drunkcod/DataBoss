using System;
using System.Collections.Generic;
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

namespace DataBoss.DataPackage
{
	class CsvRecordWriter
	{
		readonly string delimiter;
		public readonly Encoding Encoding;

		public CsvRecordWriter(string delimiter, Encoding encoding) {
			this.delimiter = delimiter;
			this.Encoding = encoding;
		}

		CsvFragmentWriter NewFragmentWriter(Stream stream) =>
			NewFragmentWriter(NewTextWriter(stream));

		CsvFragmentWriter NewFragmentWriter(TextWriter writer) =>
			new CsvFragmentWriter(new CsvWriter(writer, delimiter, leaveOpen: true));

		public void WriteHeaderRecord(Stream stream, IDataRecord data) =>
			WriteHeaderRecord(NewTextWriter(stream), data);

		TextWriter NewTextWriter(Stream stream) =>
			new StreamWriter(stream, Encoding, DataPackage.StreamBufferSize);
		
		public void WriteHeaderRecord(TextWriter writer, IDataRecord data) {
			using var csv = NewFragmentWriter(writer);
			for (var i = 0; i != data.FieldCount; ++i)
				csv.WriteField(data.GetName(i));
			csv.NextRecord();
			csv.Flush();
		}

		public void WriteRecords(TextWriter writer, IDataReader reader, DataRecordStringView view) {
			using var csv = NewFragmentWriter(writer);
			while (reader.Read())
				csv.WriteRecord(reader, view);
		}

		public Task WriteChunksAsync(ChannelReader<IReadOnlyCollection<IDataRecord>> records, ChannelWriter<Stream> chunks, DataRecordStringView view) {
			var writer = new ChunkWriter(records, chunks, this) {
				ReaderStringView = view,
			};
			return writer.RunAsync();
		}

		class CsvFragmentWriter : IDisposable
		{
			readonly CsvWriter csv;

			public CsvFragmentWriter(CsvWriter csv) {
				this.csv = csv;
			}

			public void Dispose() => csv.Dispose();

			public void WriteField(string value) => csv.WriteField(value);
			public void NextField() => csv.NextField();

			public void WriteRecord(IDataRecord r, DataRecordStringView view) {
				for (var i = 0; i != r.FieldCount; ++i) {
					if (r.IsDBNull(i))
						NextField();
					else
						WriteField(view.GetString(r, i));
				}
				NextRecord();
			}
			public void NextRecord() => csv.NextRecord();
			public void Flush() => csv.Writer.Flush();
		}

		class ChunkWriter : WorkItem
		{
			readonly ChannelReader<IReadOnlyCollection<IDataRecord>> records;
			readonly ChannelWriter<Stream> chunks;
			readonly CsvRecordWriter csv;
			readonly int bomLength;
			int chunkCapacity = DataPackage.StreamBufferSize;

			public ChunkWriter(ChannelReader<IReadOnlyCollection<IDataRecord>> records, ChannelWriter<Stream> chunks, CsvRecordWriter csv) {
				this.records = records;
				this.chunks = chunks;
				this.csv = csv;
				this.bomLength = csv.Encoding.GetPreamble().Length;
			}

			public DataRecordStringView ReaderStringView;
			public int MaxWorkers = 1;

			protected override void DoWork() {
				if (MaxWorkers == 1) {
					using var ps = new ProducerStream();
					using var result = csv.NewFragmentWriter(ps);
					chunks.Write(ps.OpenConsumer());
					foreach(var item in records.GetConsumingEnumerable())
					foreach (var r in item)
						result.WriteRecord(r, ReaderStringView);
					ps.Flush();
					//records.ForEach(WriteRecords);
				}
				else
					records.GetConsumingEnumerable().AsParallel()
						.WithDegreeOfParallelism(MaxWorkers)
						.ForAll(WriteRecords);
			}

			void WriteRecords(IReadOnlyCollection<IDataRecord> item) {
				if (item.Count == 0)
					return;

				var chunk = new MemoryStream(chunkCapacity);
				using (var fragment = csv.NewFragmentWriter(chunk))
					foreach (var r in item)
						fragment.WriteRecord(r, ReaderStringView);

				if (chunk.Position != 0) {
					chunkCapacity = Math.Max(chunkCapacity, chunk.Capacity);
					WriteChunk(chunk);
				}
			}

			void WriteChunk(MemoryStream chunk) {
				chunk.Position = bomLength;
				chunks.Write(chunk);
			}

			protected override void Cleanup() =>
				chunks.Complete();
		}
	}

}

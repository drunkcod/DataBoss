using System.Buffers;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using DataBoss.Data;
using DataBoss.DataPackage.Schema;

namespace DataBoss.DataPackage
{
    public static class TabularDataResourceCsvExtensions
    {
        public static void WriteCsv(this TabularDataResource self, TextWriter writer)
        {
            using var reader = self.Read();
            var desc = self.GetDescription();
            var view = DataReaderStringView.Create(desc.Schema.Fields, reader, null);
            var csv = new CsvRecordWriter(";", writer.Encoding);
            csv.WriteHeaderRecord(writer, reader);
            csv.WriteRecords(writer, view);
        }

        public static void WriteCsv(this TabularDataResource self, TextWriter writer, DataRecordStringViewFormatOptions options)
        {
            using var reader = self.Read();
            var desc = self.GetDescription();
            var view = DataReaderStringView.Create(desc.Schema.Fields, reader, options, null);
            var csv = new CsvRecordWriter(";", writer.Encoding);
            csv.WriteHeaderRecord(writer, reader);
            csv.WriteRecords(writer, view);
        }

        public static async Task WriteCsvAsync(this TabularDataResource self, Stream output)
        {
            using var reader = self.Read();
            var desc = self.GetDescription();
            var view = DataReaderStringView.Create(desc.Schema.Fields, reader, null);

            var csvDialect = new CsvDialectDescription { Delimiter = ";" };

            var encoding = Encoding.UTF8;
            var bom = encoding.GetPreamble();
            var csv = new CsvRecordWriter(csvDialect.Delimiter, encoding);
            if (csvDialect.HasHeaderRow)
                csv.WriteHeaderRecord(output, reader);

            var records = Channel.CreateBounded<(IMemoryOwner<IDataRecord2>, int)>(new BoundedChannelOptions(16)
            {
                SingleWriter = true,
            });

            var chunks = Channel.CreateBounded<Stream>(new BoundedChannelOptions(16)
            {
                SingleWriter = false,
                SingleReader = true,
            });

            var cancellation = new CancellationTokenSource();
            var readerTask = new RecordReader(reader.AsDataRecordReader(), records, cancellation.Token).RunAsync();
            var writerTask = csv.WriteChunksAsync(records, chunks, view);

            await Task.WhenAll(
                readerTask,
                writerTask,
                writerTask.ContinueWith(x =>
                {
                    if (x.IsFaulted)
                        cancellation.Cancel();
                }, TaskContinuationOptions.ExecuteSynchronously),
                DataPackage.CopyChunks(chunks.Reader, output, bom));
        }
    }
}

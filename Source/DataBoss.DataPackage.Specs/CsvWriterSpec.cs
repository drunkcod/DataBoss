using System.IO;
using CheckThat;
using DataBoss.DataPackage;
using Xunit;

namespace DataBoss.Specs
{
	public class CsvWriterSpec
	{
		[Fact]
		public void leave_open() {
			var ms = new MemoryStream();
			using(var csv = new CsvWriter(new StreamWriter(ms), leaveOpen: true))
			{ }
			Check.That(() => ms.CanWrite);
			
		}

		[Fact]
		public void quote_quotes() {
			var r = new StringWriter();
			using (var csv = new CsvWriter(r))
				csv.WriteField("a quote:\"");
			Check.That(() => r.ToString() == "\"a quote:\"\"\"");
		}

		[Fact]
		public void quote_delimiter() {
			var r = new StringWriter();
			using (var csv = new CsvWriter(r))
				csv.WriteField(csv.Delimiter);
			Check.That(() => r.ToString() == $"\"{CsvWriter.DefaultDelimiter}\"");
		}

		[Fact]
		public void quote_newline() {
			var r = new StringWriter();
			using (var csv = new CsvWriter(r))
				csv.WriteField("\n");
			Check.That(() => r.ToString() == $"\"\n\"");
		}

		[Fact]
		public void delimits_fields() {
			var r = new StringWriter();
			using (var csv = new CsvWriter(r)) {
				csv.WriteField("A");
				csv.WriteField("1");
			}
			Check.That(() => r.ToString() == $"A;1");
		}

		[Fact]
		public void multiple_records() {
			var r = new StringWriter();
			using (var csv = new CsvWriter(r)) {
				csv.WriteField("A");
				csv.NextRecord();
				csv.WriteField("1");
			}
			Check.That(() => r.ToString() == $"A\r\n1");
		}

	}
}

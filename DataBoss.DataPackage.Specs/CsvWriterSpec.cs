using System.IO;
using Cone;
using DataBoss.DataPackage;

namespace DataBoss.Specs
{
	[Describe(typeof(CsvWriter))]
	public class CsvWriterSpec
	{
		public void leave_open() {
			var ms = new MemoryStream();
			using(var csv = new CsvWriter(new StreamWriter(ms), leaveOpen: true))
			{ }
			Check.That(() => ms.CanWrite);
			
		}
		public void quote_quotes() {
			var r = new StringWriter();
			using (var csv = new CsvWriter(r))
				csv.WriteField("a quote:\"");
			Check.That(() => r.ToString() == "\"a quote:\"\"\"");
		}

		public void quote_delimiter() {
			var r = new StringWriter();
			using (var csv = new CsvWriter(r))
				csv.WriteField(csv.Delimiter);
			Check.That(() => r.ToString() == $"\"{CsvWriter.DefaultDelimiter}\"");
		}

		public void quote_newline() {
			var r = new StringWriter();
			using (var csv = new CsvWriter(r))
				csv.WriteField("\n");
			Check.That(() => r.ToString() == $"\"\n\"");
		}

		public void delimits_fields() {
			var r = new StringWriter();
			using (var csv = new CsvWriter(r)) {
				csv.WriteField("A");
				csv.WriteField("1");
			}
			Check.That(() => r.ToString() == $"A;1");
		}

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

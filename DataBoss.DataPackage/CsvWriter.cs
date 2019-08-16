using System;
using System.IO;

namespace DataBoss.DataPackage
{
	public class CsvWriter : IDisposable
	{
		enum WriterState : byte {
			BeginRecord = 0,
			InRecord = 1
		}

		static readonly char[] QuotableChars = new[] { '"', '\n', };

		public const string DefaultDelimiter = ";";
		const string RecordDelimiter = "\r\n";

		WriterState state;
		bool leaveOpen;
		
		public string Delimiter = DefaultDelimiter;

		public TextWriter Writer { get; }

		public CsvWriter(TextWriter output, bool leaveOpen = false) {
			this.Writer = output;
			this.leaveOpen = leaveOpen;
		}

		public void WriteField(string value) {
			NextField();

			if (ShouldQuote(value)) {
				Writer.Write('"');				
				Writer.Write(value.Replace("\"", "\"\""));
				Writer.Write('"');
			} 
			else
				Writer.Write(value);
		}

		void NextField() {
			if (state == WriterState.InRecord)
				Writer.Write(Delimiter);
			else
				state = WriterState.InRecord;
		}

		bool ShouldQuote(string value) =>
			value.IndexOfAny(QuotableChars) != -1 
			|| value.Contains(Delimiter);
		
		public void NextRecord() {
			Writer.Write(RecordDelimiter);
			state = WriterState.BeginRecord;
		}

		public void Flush() => Writer.Flush();

		void IDisposable.Dispose() { 
			Flush();
			if(!leaveOpen)
				Writer.Close();
		}
	}
}

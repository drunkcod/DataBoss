using System;
using System.IO;

namespace DataBoss.DataPackage
{
	public sealed class CsvWriter : IDisposable
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
		
		public readonly string Delimiter;

		public TextWriter Writer { get; }

		public CsvWriter(TextWriter output, string delimiter = null, bool leaveOpen = false) {
			this.Writer = output;
			this.leaveOpen = leaveOpen;
			this.Delimiter = delimiter ?? DefaultDelimiter;
		}

		public void WriteField(string value) {
			NextField();

			if (ShouldQuote(value)) {
				Writer.Write('"');
				for(var i = 0; i != value.Length; ++i) {
					var c = value[i];
					Writer.Write(c);
					if(c == '"')
						Writer.Write('"');
				}
				Writer.Write('"');
			} 
			else
				Writer.Write(value);
		}

		public void NextField() {
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

		public void Dispose() { 
			Flush();
			if(!leaveOpen)
				Writer.Close();
		}
	}
}

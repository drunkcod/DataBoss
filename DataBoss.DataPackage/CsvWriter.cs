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

		static readonly char[] QuotableChars = new[] { '"', '\n' };

		public const string DefaultDelimiter = ";";

		readonly TextWriter output;
		WriterState state;
		bool leaveOpen;
		
		public string Delimiter = DefaultDelimiter;

		public TextWriter Writer => output;

		public CsvWriter(TextWriter output, bool leaveOpen = false) {
			this.output = output;
			this.leaveOpen = leaveOpen;
		}

		public void WriteField(string value) {
			if(state == WriterState.InRecord)
				output.Write(Delimiter);
			else 
				state = WriterState.InRecord;

			if (value.IndexOfAny(QuotableChars) != -1 || value.Contains(Delimiter)) {
				output.Write('"');				
				output.Write(value.Replace("\"", "\"\""));
				output.Write('"');
			} 
			else
				output.Write(value);
		}

		public void NextRecord() {
			output.Write("\r\n");
			state = WriterState.BeginRecord;
		}

		public void Flush() => Writer.Flush();

		void IDisposable.Dispose() { 
			Flush();
			if(!leaveOpen)
				output.Close();
		}
	}
}

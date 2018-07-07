using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Linq;

namespace DataBoss.Data
{
	public class ProfiledSqlConnectionTraceWriter : IDisposable
	{
		readonly ProfiledSqlConnection connection;
		readonly TextWriter trace;

		internal  ProfiledSqlConnectionTraceWriter(ProfiledSqlConnection connection, TextWriter trace) {
			this.trace = trace;
			this.connection = connection;
			connection.CommandExecuted += OnCommandExecuted;
			connection.BulkCopyStarting += OnBulkCopyStarting;
		}

		public void Close() {
			connection.CommandExecuted -= OnCommandExecuted;
			connection.BulkCopyStarting -= OnBulkCopyStarting;
			trace.Close();
		}

		void IDisposable.Dispose() => trace.Dispose();

		void OnCommandExecuted(object _, ProfiledSqlCommandExecutedEventArgs e) {
			var p = new StringBuilder();
			var pSep = ",\r\n\t";
			foreach(SqlParameter item in e.Command.Parameters) {
				var dbType = DataBossDbType.ToDataBossDbType(item);
				p.AppendFormat("{0} {1} = {2}{3}", item.ParameterName, dbType, dbType.FormatValue(item.Value), pSep);
			}
			if (p.Length != 0) {
				p.Length -= pSep.Length;
				trace.Write("declare ");
				trace.Write(p);
				trace.Write("\r\n\r\n");
			}
			trace.Write(e.Command.CommandText);
			trace.Write("\r\ngo\r\n");
			trace.Flush();
		}

		void OnBulkCopyStarting(object _, ProfiledBulkCopyStartingEventArgs e) {
			var copyTrace = new BulkCopyTrace {
				DestinationTable = e.DestinationTable,
				Trace = trace,
			};
			e.Rows.RowRead += copyTrace.OnRowRead;
			e.Rows.Closed += copyTrace.OnClosed;
		}

		class BulkCopyTrace
		{
			public List<object[]> ReadRows = new List<object[]>();
			public string DestinationTable;
			public TextWriter Trace;

			public void OnRowRead(object sender, EventArgs e) {
				var reader = (ProfiledDataReader)sender;
				var row = new object[reader.FieldCount];
				reader.GetValues(row);
				ReadRows.Add(row);
			}

			public void OnClosed(object sender, ProfiledDataReaderClosedEventArgs e) {
				var reader = (ProfiledDataReader)sender;
				Trace.Write($"-- {e.RowCount} rows inserted into {DestinationTable} in {e.Elapsed}\r\n");
				if (ReadRows.Count != 0) {
					var columns = reader.GetColumns();
					var sb = new StringBuilder();
					sb.Append("insert ").Append(DestinationTable).Append(" values");
					foreach (var item in ReadRows) {
						sb.Append("\r\n  (");
						for (var i = 0; i != columns.Length; ++i)
							sb.Append(columns[i].ColumnType.FormatValue(item[i])).Append(", ");
						sb.Length -= 2;
						sb.Append("),");
					}
					sb.Length -= 1;
					Trace.Write(sb.Append("\r\n\r\n").ToString());
				}
				Trace.Flush();
			}
		}

		static string[] FormatParameters(DbCommand command) =>
			command.Parameters.Cast<SqlParameter>()
			.Select(p => $"{p.ParameterName} = {DataBossDbType.ToDataBossDbType(p).FormatValue(p.Value)}")
			.ToArray();
	}
}

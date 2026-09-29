using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;
using DataBoss.Data;
using DataBoss.DataPackage.Schema;

namespace DataBoss.DataPackage
{
	public class DbQueryTabularDataSource(Func<DbConnection> newConnection, string commandText, Action<DataReaderTransform>? transform = null) : ITabularDataSource
	{
		public IDataReader GetData() {
			var c = NewCommand();
			DbDataReader? reader = null;
			try {
				c.Connection!.Open();
				reader = c.ExecuteReader(CommandBehavior.CloseConnection);
				return WithTransform(reader, transform);
			}
			catch {
				Abandon(c, reader);
				throw;
			}
		}

		public async Task<IDataReader> GetDataAsync() {
			var c = NewCommand();
			DbDataReader? reader = null;
			try {
				await c.Connection!.OpenAsync();
				reader = await c.ExecuteReaderAsync(CommandBehavior.CloseConnection);
				return WithTransform(reader, transform);
			}
			catch {
				Abandon(c, reader);
				throw;
			}
		}

		DbCommand NewCommand() {
			var db = newConnection();
			db.DisposeOnClose();
			var c = db.CreateCommand();
			c.CommandText = commandText;
			return c;
		}

		// Until a reader has been handed to the caller we own the command and connection.
		static void Abandon(DbCommand c, DbDataReader? reader) {
			var connection = c.Connection;
			reader?.Dispose();
			c.Dispose();
			connection?.Dispose();
		}

		static DbDataReader WithTransform(DbDataReader r, Action<DataReaderTransform>? transform) {
			if (transform is null) return r;
			return r.WithTransform(transform);
		}
	}
}

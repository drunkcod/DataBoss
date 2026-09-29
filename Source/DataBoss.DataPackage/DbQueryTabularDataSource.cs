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
			c.Connection!.Open();
			return WithTransform(c.ExecuteReader(CommandBehavior.CloseConnection), transform);
		}

		public async Task<IDataReader> GetDataAsync() {
			var c = NewCommand();
			await c.Connection!.OpenAsync();
			return WithTransform(await c.ExecuteReaderAsync(CommandBehavior.CloseConnection), transform);
		}

		DbCommand NewCommand() {
			var db = newConnection();
			db.DisposeOnClose();
			var c = db.CreateCommand();
			c.CommandText = commandText;
			return c;
		}

		static DbDataReader WithTransform(DbDataReader r, Action<DataReaderTransform>? transform) {
			if (transform is null) return r;
			return r.WithTransform(transform);
		}
	}
}

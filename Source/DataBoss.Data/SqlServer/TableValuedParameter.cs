using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace DataBoss.Data.SqlServer
{
	public interface ITableValuedParameter
	{
		string TypeName { get; }
		object Rows { get; }
	}

	public class TableValuedParameter : ITableValuedParameter
	{
		public string TypeName { get; }
		public object Rows { get; }

		TableValuedParameter(string typeName, object rows) {
			this.TypeName = typeName;
			this.Rows = rows;
		}

		public static TableValuedParameter Create(string name, DataTable rows) => new TableValuedParameter(name, rows);
		public static TableValuedParameter Create(string name, DbDataReader rows) => new TableValuedParameter(name, rows);
		public static TableValuedParameter Create<T>(string name, IEnumerable<T> rows) => new TableValuedParameter(name, rows.ToDataReader());
	}
}

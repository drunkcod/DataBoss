using System;
using System.Data;
using System.Data.Common;

namespace DataBoss.Data
{
	class WhereDataReader : DataReaderDecoratorBase
	{
		readonly Func<IDataRecord, bool> predicate;

		public WhereDataReader(DbDataReader inner, Func<IDataRecord, bool> predicate) : base(inner) {
			this.predicate = predicate;
		}

		public override bool Read() {
			while(base.Read())
				if(predicate(this))
					return true;
			return false;
		}
	}
}

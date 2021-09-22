using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using DataBoss.Linq;

namespace DataBoss.Data
{
	public class MultiDataReader : DataReaderDecoratorBase
	{ 
		readonly IReadOnlyList<DbDataReader> readers;
		readonly int[] nextNext;
		int next;

		public MultiDataReader(IReadOnlyList<DbDataReader> readers) : base(readers[0]) { 
			this.readers = readers;
			this.nextNext = new int[readers.Count];
			for(var i = 0; i != readers.Count; ++i)
				this.nextNext[i] = (i + 1) % readers.Count;
			this.next = readers.Count - 1;
		}

		public override bool IsClosed => readers.All(x => x.IsClosed);

		public override void Close() => readers.ForEach(x => x.Close());
		protected override void Dispose(bool disposing) 
		{
			if(!disposing)
				return;

			readers.ForEach(x => x.Dispose());
		}

		public override bool NextResult() => false;

		public override bool Read() {
			for(;;) {
				var current = nextNext[next];
				if (readers[current].Read()) {
					Inner = readers[current];
					next = current;
					return true;
				} else {
					if(current == nextNext[current])
						return false;
					nextNext[next] = nextNext[current];
				}
			}
		}
	}
}

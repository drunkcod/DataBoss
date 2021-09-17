using System;
using System.Data;

namespace DataBoss.Data
{
	public class DataReaderDecorator : DataReaderDecoratorBase
	{
		public DataReaderDecorator(IDataReader inner) : base(inner) {
			this.GetName = inner.GetName;
		}

		public Func<int, string> GetName; 
		public event Action<IDataRecord> RecordRead;
		public event Action Closed;
		public event Action Disposed;

		public override void Close() {
			base.Close();
			Closed?.Invoke();
		}

		protected override void Dispose(bool disposing) {
			base.Dispose(disposing);
			if(!disposing)
				return;

			if(!IsClosed)
				Close();
			Disposed?.Invoke();
		}

		public override bool Read() {
			if(base.Read()) {
				RecordRead?.Invoke(this);
				return true;
			}
			return false;
		}
	}
}

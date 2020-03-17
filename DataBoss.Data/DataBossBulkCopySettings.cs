namespace DataBoss.Data
{
	using System;

	[Flags]//this mimics SqlBulkCopyOptions
	public enum DataBossBulkCopyOptions
	{
		Default = 0,
		KeepIdentity = 1,
		CheckConstraints = 2,
		TableLock = 4,
		KeepNulls = 8,
		FireTriggers = 16,
		UseInternalTransaction = 32
	}

	public struct DataBossBulkCopySettings
	{
		public int? BatchSize;
		public int? CommandTimeout;
		public DataBossBulkCopyOptions? Options;

		public DataBossBulkCopySettings WithCommandTimeout(int? value) {
			var x = this;
			x.CommandTimeout = value;
			return x;
		}
	}
}
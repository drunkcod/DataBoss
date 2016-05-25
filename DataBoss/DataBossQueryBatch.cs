namespace DataBoss
{
	public enum DataBossQueryBatchType
	{
		Query,
		ExternalCommand,
	}

	public struct DataBossQueryBatch
	{
		readonly string batch;

		public readonly DataBossQueryBatchType BatchType;

		public static DataBossQueryBatch Query(string query) {
			return new DataBossQueryBatch(DataBossQueryBatchType.Query, query);
		}

		public static DataBossQueryBatch ExternalCommand(string command) {
			return new DataBossQueryBatch(DataBossQueryBatchType.ExternalCommand, command);
		}

		public DataBossQueryBatch(DataBossQueryBatchType batchType, string batch) {
			this.BatchType = batchType;
			this.batch = batch;
		}

		public override string ToString() => batch;
	}
}
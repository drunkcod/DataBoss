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
		public readonly string Path;

		public readonly DataBossQueryBatchType BatchType;

		public static DataBossQueryBatch Query(string query, string path) {
			return new DataBossQueryBatch(DataBossQueryBatchType.Query, query, path);
		}

		public static DataBossQueryBatch ExternalCommand(string command, string path) {
			return new DataBossQueryBatch(DataBossQueryBatchType.ExternalCommand, command, path);
		}

		public DataBossQueryBatch(DataBossQueryBatchType batchType, string batch, string path) {
			this.BatchType = batchType;
			this.batch = batch;
			this.Path = path;
		}

		public override string ToString() => batch;
	}
}
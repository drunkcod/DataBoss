using System;
using CheckThat;
using Xunit;

namespace DataBoss
{
	public class DataBoss_ : IClassFixture<SqlServerFixture>
	{
		readonly SqlServerFixture db;

		public DataBoss_(SqlServerFixture db) {
			this.db = db;
		}

		class DataBossTestConfig : IDataBossConfiguration
		{
			public string Script => throw new NotImplementedException();

			public string DefaultSchema { get; set; }

			public string ConnectionString;
			public string GetConnectionString() => ConnectionString;

			public IDataBossMigration GetTargetMigration() {
				throw new NotImplementedException();
			}
		}

		[Fact]
		public void ensures_matching_default_schema() {
			var dataBoss = DataBoss.Create(new DataBossTestConfig {
				ConnectionString = db.ConnectionString,
				DefaultSchema = "DEFALT",
			}, new NullDataBossLog());

			Check.Exception<InvalidOperationException>(() => dataBoss.Initialize());
		}
	}
}

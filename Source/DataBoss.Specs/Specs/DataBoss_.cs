using System;
using System.Data;
using CheckThat;
using Microsoft.Data.SqlClient;
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

			public string? DefaultSchema { get; set; }

			public string? ConnectionString;
			public string GetConnectionString() => ConnectionString ?? throw new InvalidOperationException();
			public IDbConnection GetDbConnection() => new SqlConnection(ConnectionString);

			public IDataBossMigration GetTargetMigration() {
				throw new NotImplementedException();
			}

			public string Database => "";
			public string Server => "localhost";
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

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using CheckThat;
using DataBoss.Data;
using DataBoss.Testing.SqlServer;
using Xunit;

namespace DataBoss.Testing
{
	public sealed class SqlServerTestDb_ : IClassFixture<SqlServerFixture>, IDisposable
	{
		readonly List<IDisposable> garbage = new();

		void IDisposable.Dispose() {
			garbage.ForEach(x => x.Dispose());
		}

		T Cleanup<T>(T item) where T : IDisposable {
			garbage.Add(item);
			return item;
		}

		readonly string ConnectionString;
		readonly TestDbConfig Config;

		public SqlServerTestDb_(SqlServerFixture db) {
			ConnectionString = db.ConnectionString;
			Config = db.Config;
		}

		[Fact]
		public void creates_instance_on_first_request() {
			var name = Guid.NewGuid().ToString();

			Check.That(() => CountDatabasesByName(name) == 0);
			Cleanup(SqlServerTestDb.GetOrCreate(Config.WithName(name)));
			Check.That(() => CountDatabasesByName(name) == 1);			
		}

		[Fact]
		public void instance_is_created_only_once() {
			var name = Guid.NewGuid().ToString();
			Check.That(() => Cleanup(SqlServerTestDb.GetOrCreate(Config.WithName(name))) == Cleanup(SqlServerTestDb.GetOrCreate(Config.WithName(name))));
		}

		[Fact]
		public void is_deleted_when_disposed() {
			var db = Cleanup(SqlServerTestDb.Create(Config));
			Check.That(() => CountDatabasesByName(db.Name) == 1);
			db.Dispose();
			Check.That(() => CountDatabasesByName(db.Name) == 0);
		}

		int CountDatabasesByName(string name) => 
			(int)ExecuteScalar(
				"select count(*) from master.sys.databases where name = @name", 
				new { name });

		object ExecuteScalar<T>(string query, T args) {
			using var db = new SqlConnection(ConnectionString);
			db.Open();
			return db.ExecuteScalar(query, args);
		}
	}
}

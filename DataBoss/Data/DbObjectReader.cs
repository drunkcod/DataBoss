using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace DataBoss.Data
{
	public class DbObjectQuery
	{
		readonly Func<SqlCommand> getCommand;
		readonly ObjectReader reader;

		public DbObjectQuery(Func<SqlCommand> getCommand, ObjectReader reader)
		{
			this.getCommand = getCommand;
			this.reader = reader;
		}

		public IEnumerable<TOutput> Read<TOutput>() where TOutput : new()
		{
			using (var q = getCommand())
			using (var r = q.ExecuteReader())
				foreach (var row in reader.Read<TOutput>(r))
					yield return row;
		}

		public TOutput Single<TOutput>() where TOutput : new() =>
			Read<TOutput>().Single();
	}

	public class DbObjectReader
	{
		readonly ObjectReader reader = new ObjectReader();
		readonly SqlConnection db;

		public IEnumerable<T> Read<T>(string command) where T : new() => Query(command, new {}).Read<T>();

		public T Single<T>(string command) where T : new() => Read<T>(command).Single();

		public DbObjectQuery Query<T>(string command, T args) =>
			new DbObjectQuery(() => db.CreateCommand(command, args), reader);

		public DbObjectReader(SqlConnection db) {
			this.db = db;
		}	
	}}

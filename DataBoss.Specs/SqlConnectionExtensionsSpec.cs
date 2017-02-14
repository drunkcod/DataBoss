﻿using Cone;
using DataBoss.Data;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataBoss.Specs
{
	[Describe(typeof(SqlConnectionExtensions))]
	public class SqlConnectionExtensionsSpec
	{
		SqlConnection Connection;

		[BeforeEach]
		public void BeforeEach() { 
			Connection = new SqlConnection(Testing.DatabaseSetup.GetTemporaryInstance("DataBoss").ToString());
			Connection.Open();
		}

		[AfterAll]
		public void AfterAll() { 
			Connection.Dispose();
			Connection = null;	
		}

		class MyStuffRow
		{
			public int Id;
			public string Value;
		}

		public void insert_and_retreive_ids_is_zippable() { 
			var destinationTableName = "#MyStuff";
			Connection.ExecuteNonQuery($"create table {destinationTableName}(Id int identity, Value varchar(max) not null,)");

			var newItems = new[]{ 
				new { Value = "First" },
				new { Value = "Second" },
				new { Value = "Third" },
			};
			var newIds = Connection.InsertAndGetIdentities(destinationTableName, newItems);
			var myStuff = new DbObjectReader(Connection).Read<MyStuffRow>($"select * from {destinationTableName}");

			Check.With(() => newIds.Zip(myStuff, (x, item) => new { SequenceId = x, item.Id, item.Value }).ToList())
				.That(
					zipped => zipped[0] == new { SequenceId = 1, Id = 1, Value = "First" },
					zipped => zipped[1] == new { SequenceId = 2, Id = 2, Value = "Second" },
					zipped => zipped[2] == new { SequenceId = 3, Id = 3, Value = "Third" });
		}
	}
}
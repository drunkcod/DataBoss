using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Cone;

namespace DataBoss.Specs
{
	[Describe(typeof(DataBossMigrator))]
	public class DataBossMigratorSpec
	{
		class TestableMigrationScope : IDataBossMigrationScope
		{
			public void Dispose() { }

			public List<string> ExecutedQueries = new List<string>(); 
			public Action<string> OnExecute;

			public event EventHandler<ErrorEventArgs> OnError;
			public bool IsFaulted { get; set; }

			public void Begin(DataBossMigrationInfo info) {}

			public void Execute(string query) {
				if(OnExecute != null)
					OnExecute(query);
				ExecutedQueries.Add(query);
			}

			public void Done() { }
		}

		public void stops_application_when_migration_scope_is_faulted() {
			var scope = new TestableMigrationScope();
			var migrator = new DataBossMigrator(_ => scope);

			scope.OnExecute += _ => scope.IsFaulted = true;
			Check.That(() => migrator.ApplyRange(new[] {
				TextMigration("First!"),
				TextMigration("Second!"),
			}) == false);

			Check.That(
				() => scope.ExecutedQueries.Count == 1,
				() => scope.ExecutedQueries.SequenceEqual(new[] { "First!" }));
		}

		IDataBossMigration TextMigration(string s) {
			return new DataBossTextMigration(() => new StringReader(s));
		}
	}
}

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace DataBoss.Migrations
{
	public class DataBossDirectoryMigration : IDataBossMigration
	{
		static readonly Regex IdNameEx = new Regex(@"(?<id>\d+)(?<name>.*?)(\.(sql|cmd))?$", RegexOptions.IgnoreCase | RegexOptions.Compiled);
		static readonly int IdGroup = IdNameEx.GroupNumberFromName("id");
		static readonly int NameGroup = IdNameEx.GroupNumberFromName("name");

		readonly string path;
		readonly string childContext;

		public DataBossDirectoryMigration(string path, DataBossMigrationInfo info) : this(path, info, info.Context)
		{}

		DataBossDirectoryMigration(string path, DataBossMigrationInfo info, string childContext) {
			this.path = path;
			this.Info = info;
			this.childContext = childContext;
		}

		public string Path => path;

		public IEnumerable<IDataBossMigration> GetSubMigrations() =>
			GetFileMigrations()
			.Concat(GetDirectoryMigrations())
			.OrderBy(x => x.Info.Id);

		IEnumerable<IDataBossMigration> GetFileMigrations() =>
			GetMigrations(Directory.GetFiles(path, "*"))
			.Select(x => {
				switch(System.IO.Path.GetExtension(x.Key).ToLower()) {
					default: throw new ArgumentException($"Unsupported migration {x.Key}");
					case ".sql": return (IDataBossMigration)new DataBossQueryMigration(x.Key, () => File.OpenText(x.Key), x.Value);
					case ".cmd": return new DataBossExternalCommandMigration(x.Key, () => File.OpenText(x.Key), x.Value);
				}
			});

		IEnumerable<IDataBossMigration> GetDirectoryMigrations() => 
			GetMigrations(Directory.GetDirectories(path))
			.Select(x => new DataBossDirectoryMigration(
				x.Key,
				x.Value,
				string.IsNullOrEmpty(childContext) ? x.Value.Id.ToString() : childContext + "." + x.Value.Id
			));

		public IEnumerable<KeyValuePair<string, DataBossMigrationInfo>> GetMigrations(string[] paths) {
			for(var i = 0; i != paths.Length; ++i) {
				var m = IdNameEx.Match(System.IO.Path.GetFileName(paths[i]));
				if(m.Success)
					yield return new KeyValuePair<string, DataBossMigrationInfo>(
						paths[i], GetMigrationInfo(m)
					);
			}
		}

		DataBossMigrationInfo GetMigrationInfo(Match m) =>
			new DataBossMigrationInfo {
				Id = long.Parse(m.Groups[IdGroup].Value),
				Context = childContext,
				Name = m.Groups[NameGroup].Value.Trim(),
			};

		public DataBossMigrationInfo Info { get; }
		public bool HasQueryBatches => false;
		public IEnumerable<DataBossQueryBatch> GetQueryBatches() => Enumerable.Empty<DataBossQueryBatch>();
	}
}
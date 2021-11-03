using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
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
		readonly bool isRepeatable;

		public DataBossDirectoryMigration(string path, DataBossMigrationInfo info, bool isRepeatable) : this(path, info, info.Context, isRepeatable)
		{}

		DataBossDirectoryMigration(string path, DataBossMigrationInfo info, string childContext, bool isRepeatable) {
			this.path = path;
			this.Info = info;
			this.childContext = childContext;
			this.isRepeatable = isRepeatable;
		}

		public string Path => path;

		public IEnumerable<IDataBossMigration> GetSubMigrations() =>
			GetFileMigrations()
			.Concat(GetDirectoryMigrations())
			.OrderBy(x => x.Info.Id);

		IEnumerable<IDataBossMigration> GetFileMigrations() {
			using var sha256 = SHA256.Create();
			foreach(var item in GetMigrations(Directory.GetFiles(path, "*"), x => {
				using var bytes = File.OpenRead(x);
				return sha256.ComputeHash(bytes);
			})
			.Select(x => System.IO.Path.GetExtension(x.Key).ToLower() switch {
				".sql" => (IDataBossMigration)new DataBossQueryMigration(x.Key, () => File.OpenText(x.Key), x.Value, IsRepeatable),
				".cmd" => new DataBossExternalCommandMigration(x.Key, () => File.OpenText(x.Key), x.Value, IsRepeatable),
				_ => throw new ArgumentException($"Unsupported migration {x.Key}"),
			}))
				yield return item;
		}

		IEnumerable<IDataBossMigration> GetDirectoryMigrations() => 
			GetMigrations(Directory.GetDirectories(path))
			.Select(x => new DataBossDirectoryMigration(
				x.Key,
				x.Value,
				string.IsNullOrEmpty(childContext) ? x.Value.Id.ToString() : childContext + "." + x.Value.Id,
				isRepeatable));

		public IEnumerable<(string Key, DataBossMigrationInfo Value)> GetMigrations(string[] paths, Func<string, byte[]> computeHash = null) {
			computeHash ??= _ => null;
			for(var i = 0; i != paths.Length; ++i) {
				var m = IdNameEx.Match(System.IO.Path.GetFileName(paths[i]));
				if(m.Success) {
					var info = GetMigrationInfo(m);
					info.MigrationHash = computeHash(paths[i]);
					yield return (paths[i], info);
				}
			}
		}

		public DataBossMigrationInfo GetMigrationInfo(string path) {
			var m = IdNameEx.Match(System.IO.Path.GetFileName(path));
			return m.Success ? GetMigrationInfo(m) : throw new InvalidOperationException();
		}

		DataBossMigrationInfo GetMigrationInfo(Match m) =>
			new DataBossMigrationInfo {
				Id = long.Parse(m.Groups[IdGroup].Value),
				Context = childContext,
				Name = m.Groups[NameGroup].Value.Trim(),
			};

		public DataBossMigrationInfo Info { get; }
		public bool HasQueryBatches => false;
		public bool IsRepeatable => isRepeatable;
		public IEnumerable<DataBossQueryBatch> GetQueryBatches() => Enumerable.Empty<DataBossQueryBatch>();
	}
}
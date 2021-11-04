using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using DataBoss.Linq;
using IoPath = System.IO.Path;

namespace DataBoss.Migrations
{
	public class DataBossDirectoryMigration : IDataBossMigration
	{
		static readonly Regex IdNameEx = new(@"(?<id>\d+)(?<name>.*?)(\.(sql|cmd))?$", RegexOptions.IgnoreCase | RegexOptions.Compiled);
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
			SHA256 sha256 = null;
			try {
				foreach(var item in GetMigrations(Directory.GetFiles(path, "*"), x => {
					using var bytes = File.OpenRead(x);
					return (sha256 ??= SHA256.Create()).ComputeHash(bytes);
				}))
					yield return IoPath.GetExtension(item.Path).ToLower() switch {
						".sql" => new DataBossQueryMigration(item.Path, () => File.OpenText(item.Path), item.Info, IsRepeatable),
						".cmd" => new DataBossExternalCommandMigration(item.Path, () => File.OpenText(item.Path), item.Info, IsRepeatable),
						_ => throw new ArgumentException($"Unsupported migration {item.Path}"),
					};
			} finally {
				sha256?.Dispose();
			}
		}

		IEnumerable<IDataBossMigration> GetDirectoryMigrations() => 
			GetMigrations(Directory.GetDirectories(path))
			.Select(x => new DataBossDirectoryMigration(
				x.Path,
				x.Info,
				string.IsNullOrEmpty(childContext) ? x.Info.Id.ToString() : childContext + "." + x.Info.Id,
				isRepeatable));

		public IEnumerable<(string Path, DataBossMigrationInfo Info)> GetMigrations(string[] paths, Func<string, byte[]> computeHash = null) {
			computeHash ??= _ => null;
			return paths
				.ConvertAll(x => (Path: x, Match: IdNameEx.Match(IoPath.GetFileName(x))))
				.Where(x => x.Match.Success)
				.Select(x => (x.Path, info: GetMigrationInfo(x.Match, computeHash(x.Path))));
		}

		public DataBossMigrationInfo GetMigrationInfo(string path) {
			var m = IdNameEx.Match(IoPath.GetFileName(path));
			return m.Success ? GetMigrationInfo(m, null) : throw new InvalidOperationException();
		}

		DataBossMigrationInfo GetMigrationInfo(Match m, byte[] migrationHash) =>
			new() {
				Id = long.Parse(m.Groups[IdGroup].Value),
				Context = childContext,
				Name = m.Groups[NameGroup].Value.Trim(),
				MigrationHash  = migrationHash,
			};

		public DataBossMigrationInfo Info { get; }
		public bool HasQueryBatches => false;
		public bool IsRepeatable => isRepeatable;
		public IEnumerable<DataBossQueryBatch> GetQueryBatches() => Enumerable.Empty<DataBossQueryBatch>();
	}
}
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace DataBoss.Migrations
{
	public class DataBossDirectoryMigration : IDataBossMigration
	{
		static readonly Regex IdNameEx = new Regex(@"(?<id>\d+)(?<name>.*)$");
		static readonly int IdGroup = IdNameEx.GroupNumberFromName("id");
		static readonly int NameGroup = IdNameEx.GroupNumberFromName("name");

		readonly string path;
		readonly DataBossMigrationInfo info;
		readonly string childContext;

		public DataBossDirectoryMigration(string path, DataBossMigrationInfo info) : this(path, info, info.Context)
		{}

		DataBossDirectoryMigration(string path, DataBossMigrationInfo info, string childContext) {
			this.path = path;
			this.info = info;
			this.childContext = childContext;
		}

		public IEnumerable<IDataBossMigration> GetSubMigrations() {
			return
				GetTextMigrations()
					.Concat(GetDirectoryMigraions())
					.OrderBy(x => x.Info.Id);
		}

		IEnumerable<IDataBossMigration> GetTextMigrations() {
			return Directory.GetFiles(path, "*.sql")
				.ConvertAll(x => new {
					m = IdNameEx.Match(Path.GetFileNameWithoutExtension(x)),
					path = x,
				}).Where(x => x.m.Success)
				.Select(x => new DataBossTextMigration(() => File.OpenText(x.path)) {
					Info = GetMigrationInfo(x.m),
				});
		}

		IEnumerable<IDataBossMigration> GetDirectoryMigraions() {
			return Directory.GetDirectories(path)
				.ConvertAll(x => new {
					m = IdNameEx.Match(Path.GetFileName(x)),
					path = x
				}).Where(x => x.m.Success)
				.Select(x => new {
					x.path,
					info = GetMigrationInfo(x.m),
				})
				.Select(x => new DataBossDirectoryMigration(
					x.path, 
					x.info,
					string.IsNullOrEmpty(childContext) ? x.info.Id.ToString() : childContext + "." + x.info.Id
					));
		}

		DataBossMigrationInfo GetMigrationInfo(Match m) {
			return new DataBossMigrationInfo {
				Id = long.Parse(m.Groups[IdGroup].Value),
				Context = childContext,
				Name = m.Groups[NameGroup].Value.Trim(),
			};
		}

		public DataBossMigrationInfo Info => info;

		public bool HasQueryBatches => false;

		public IEnumerable<DataBossQueryBatch> GetQueryBatches() { yield break; }
	}
}
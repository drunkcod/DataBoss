using System.Xml.Serialization;
using IoPath = System.IO.Path;

namespace DataBoss.Migrations
{
	public class DataBossMigrationPath
	{
		[XmlAttribute("context")]
		public string Context;

		[XmlAttribute("path")]
		public string Path;

		[XmlAttribute("repeatable")]
		public bool IsRepeatable;

		public DataBossMigrationPath WithRootPath(string rootPath) => new(){
			Context = Context,
			Path = IoPath.Combine(rootPath, GetOsPath()),
			IsRepeatable = IsRepeatable,
		};

		public string GetOsPath() => IoPath.GetPathRoot(Path) + IoPath.Combine(Path.Split(new[]{'\\', '/' }, System.StringSplitOptions.RemoveEmptyEntries));
	}
}
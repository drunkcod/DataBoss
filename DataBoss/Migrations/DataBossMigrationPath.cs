using System.Xml.Serialization;

namespace DataBoss.Migrations
{
	public class DataBossMigrationPath
	{
		[XmlAttribute("context")]
		public string Context;

		[XmlAttribute("path")]
		public string Path;
	}
}
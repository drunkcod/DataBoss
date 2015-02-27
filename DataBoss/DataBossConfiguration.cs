using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace DataBoss
{
	[XmlRoot("db")]
	public class DataBossConfiguration
	{
		[XmlElement("connectionString")]
		public string ConnectionString;

		[XmlElement("migrations")]
		public DataBossMigrationPath Migration;

		public static DataBossConfiguration Load(string path) {
			var xml = new XmlSerializer(typeof(DataBossConfiguration));
			using(var input = File.OpenRead(path))
				return (DataBossConfiguration)xml.Deserialize(input);
		}
	}
}

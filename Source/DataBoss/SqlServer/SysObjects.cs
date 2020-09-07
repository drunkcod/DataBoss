using System.ComponentModel.DataAnnotations.Schema;

namespace DataBoss.SqlServer
{
	[Table("sys.objects")]
	public class SysObjects
	{
		[Column("name")]
		public string Name;
	}
}

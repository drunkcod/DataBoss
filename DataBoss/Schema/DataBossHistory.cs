using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using DataBoss.Data.Scripting;

namespace DataBoss.Schema
{
	[Table("__DataBossHistory", Schema = "dbo")]
	public class DataBossHistory
	{
		[Column(Order = 0),Key]
		public long Id;
		[Column(Order = 1), Key, Required, MaxLength(64)]
		public string Context;
		[Column(Order = 2), Required]
		public string Name;
		[Column(Order = 3), Clustered]
		public DateTime StartedAt;
		[Column(Order = 4)]
		public DateTime? FinishedAt;
		[Column(Order = 5)]
		public string User;
	}
}

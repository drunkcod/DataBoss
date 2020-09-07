using System.ComponentModel.DataAnnotations;

namespace DataBoss.Data.Common
{
	#pragma warning disable CS0649
	public struct IdRow<T> 
	{
		[Required]
		public T Id; 
	}
	#pragma warning restore CS0649
}
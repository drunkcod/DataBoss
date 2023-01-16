using CheckThat;
using Xunit;

namespace DataBoss
{
	public class Varint_
	{
		[Theory]
		[InlineData(ulong.MaxValue, 10)]
		[InlineData(128, 2)]
		public void encoding_length(ulong value, int length) => 
			Check.That(() => Varint.GetBytes(value).Length == length);
	}
}

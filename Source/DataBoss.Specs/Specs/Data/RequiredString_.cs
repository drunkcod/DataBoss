using CheckThat;
using Xunit;

namespace DataBoss.Data
{
	public class RequiredString_
	{
		[Fact]
		public void default_equals_empty_string() =>
			Check.That(() => default(RequiredString) == string.Empty);
	}
}

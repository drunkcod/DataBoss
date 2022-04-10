using System.Linq;
using CheckThat;
using Xunit;

namespace DataBoss.Data
{
	public class IdOf_
	{
		const int SomeValue = 12345;

		class MyClass { }

		[Fact]
		public void looks_like_a_int() => Check
			.With(() => new IdOf<MyClass>(SomeValue))
			.That(
				id => id.ToString() == SomeValue.ToString(),
				id => id.GetHashCode() == SomeValue.GetHashCode(),
				id => id == (IdOf<MyClass>)SomeValue);

		[Fact]
		public void is_sortable() => Check
			.With(() => new[] { (IdOf<MyClass>)3, (IdOf<MyClass>)1, (IdOf<MyClass>)2, }.OrderBy(x => x).ToArray())
			.That(xs => (int)xs[0] == 1);
	}
}

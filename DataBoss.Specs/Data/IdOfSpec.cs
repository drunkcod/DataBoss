using System.Linq;
using Cone;

namespace DataBoss.Data.Specs
{

	[Describe(typeof(IdOf<>))]
	public class IdOfSpec
	{
		class MyClass { }

		public void looks_like_a_int() {
			var value = 12345;
			Check.With(() => new IdOf<MyClass>(value))
			.That(
				id => id.ToString() == value.ToString(),
				id => id.GetHashCode() == value.GetHashCode(),
				id => id == (IdOf<MyClass>)value);
		}

		public void is_sortable() =>
			Check.With(() => new[] { (IdOf<MyClass>)3, (IdOf<MyClass>)1, (IdOf<MyClass>)2, }.OrderBy(x => x).ToArray())
			.That(xs => (int)xs[0] == 1);
	}
}

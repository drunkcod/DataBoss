using Cone;

namespace DataBoss.Specs.Data
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
	}
}

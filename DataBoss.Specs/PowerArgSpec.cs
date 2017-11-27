using System;
using System.ComponentModel.DataAnnotations;
using Cone;

namespace DataBoss.Specs
{
	[Describe(typeof(PowerArg))]
	public class PowerArgSpec
	{
		#pragma warning disable CS0649
		class MyArgs
		{
			[Required]
			public string Required;

			public int Optional;
		}
		#pragma warning restore CS0649

		public void is_required_if_has_RequiredAttribue() =>
			Check.With(() => new PowerArg(typeof(MyArgs).GetMember(nameof(MyArgs.Required))[0]))
			.That(arg => arg.IsRequired);

		public void non_required_attribute_is_optional() =>
			Check.With(() => new PowerArg(typeof(MyArgs).GetMember(nameof(MyArgs.Optional))[0]))
			.That(arg => !arg.IsRequired);

		[Row(typeof(MyArgs), nameof(MyArgs.Required), "-Required <required>")]
		[Row(typeof(MyArgs), nameof(MyArgs.Optional), "[-Optional <optional>]")]
		public void formatting(Type argType, string member, string expected) =>
			Check.That(() => new PowerArg(argType.GetMember(member)[0]).ToString() == expected);
	}
}

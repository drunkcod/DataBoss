using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using CheckThat;
using Xunit;

namespace DataBoss.Specs
{
	public class PowerArgSpec
	{
		#pragma warning disable CS0649
		class MyArgs
		{
			[Required]
			public string Required;

			public int Optional;

			[Required, PowerArg(Hint = "foo goes here")]
			public string Foo;

			[Required, RegularExpression("xml|json")]
			public string Format;
		}
		#pragma warning restore CS0649

		[Fact]
		public void is_required_if_has_RequiredAttribue() => Check
			.With(() => new PowerArg(typeof(MyArgs).GetMember(nameof(MyArgs.Required))[0]))
			.That(arg => arg.IsRequired);

		[Fact]
		public void non_required_attribute_is_optional() => Check
			.With(() => new PowerArg(typeof(MyArgs).GetMember(nameof(MyArgs.Optional))[0]))
			.That(arg => !arg.IsRequired);

		[Theory]
		[InlineData(typeof(MyArgs), nameof(MyArgs.Required), "-Required <required>")]
		[InlineData(typeof(MyArgs), nameof(MyArgs.Optional), "[-Optional <optional>]")]
		[InlineData(typeof(MyArgs), nameof(MyArgs.Foo), "-Foo <foo goes here>")]
		[InlineData(typeof(MyArgs), nameof(MyArgs.Format), "-Format <xml|json>")]
		public void formatting(Type argType, string member, string expected) =>
			Check.That(() => new PowerArg(argType.GetMember(member)[0]).ToString() == expected);

		#pragma warning disable CS0649
		class DescribedArg
		{
			[Description("This is THE arg")]
			public int TheArg;
		}
		#pragma warning restore CS0649

		[Fact]
		public void description() => Check
			.With(() => new PowerArg(typeof(DescribedArg).GetMember(nameof(DescribedArg.TheArg))[0]))
			.That(theArg => theArg.Description == "This is THE arg");
	}
}

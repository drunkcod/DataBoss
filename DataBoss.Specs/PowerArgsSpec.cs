using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using Cone;

namespace DataBoss.Specs
{
	[Describe(typeof(PowerArgs))]
	public class PowerArgsSpec
	{
		public void uses_key_value_pairs() {
			var args = PowerArgs.Parse("-Foo", "Bar");
			Check.That(
				() => args.Count == 1, 
				() => args["Foo"] == "Bar");
		}

		public void merges_comma_separated_values_to_one()
		{
			var args = PowerArgs.Parse(
				"-1", "A,", "B",
				"-2", "A", ",B",
				"-3", "A", ",", "B",
				"-4", "A,", "B,", "C"
			);
			Check.That(
				() => args.Count == 4, 
				() => args["1"] == "A,B",
				() => args["2"] == "A,B",
				() => args["3"] == "A,B",
				() => args["4"] == "A,B,C");
		}

		public void missing_args_yields_InvalidOperationException() {
			Check.Exception<InvalidOperationException>(() => PowerArgs.Parse("-Foo"));
		}

		public void captures_non_options() {
			var args = PowerArgs.Parse("Hello", "World");
			Check.That(
				() => args.Commands.Count == 2, 
				() => args.Commands[0] == "Hello",
				() => args.Commands[1] == "World");
		}

		public void can_TryGetArg() {
			var args = PowerArgs.Parse("-Foo", "Bar");
			string value;
			Check.That(
				() => args.TryGetArg("Foo", out value), 
				() => !args.TryGetArg("Bar", out value));
		}

		class MyArgs
		{
			public string MyField;
			public string MyProp { get; set; }
		}

		public void can_fill_field() {
			Check.That(() => PowerArgs.Parse("-MyField", "Value").Into<MyArgs>().MyField == "Value");
		}

		public void can_fill_property() {
			Check.That(() => PowerArgs.Parse("-MyProp", "Value").Into<MyArgs>().MyProp == "Value");
		}

		public void can_fill_existing_instance() {
			Check.That(() => PowerArgs.Parse("-MyProp", "NewValue").Into(new MyArgs { MyProp = "Prop", MyField = "Field" }).MyProp == "NewValue");
		}

		class MyArgsWithDefaults
		{
			[DefaultValue("42")]
			public string TheAnswer;
		}

		public void uses_defaults_if_not_specified() {
			Check.That(() => PowerArgs.Parse().Into<MyArgsWithDefaults>().TheAnswer == "42");
		}

		class MyArgsWithNonStrings
		{
			public List<int> MyList;
			public int MyInt;
			public DateTime MyDateTime;
			public DateTime? MaybeDateTime;
		}

		public void ignores_non_stringable_members() {
			Check.That(
				() => PowerArgs.Parse("-MyList", "42").Into<MyArgsWithNonStrings>().MyList == null);
		}

		public void attempts_to_parse_DateTime() {
			Check.That(
				() => PowerArgs.Parse("-MyDateTime", "2016-02-29").Into<MyArgsWithNonStrings>().MyDateTime == new DateTime(2016, 02, 29));
		}

		public void parses_nullables() {
			Check.That(
				() => PowerArgs.Parse("-MaybeDateTime", "2016-02-29").Into<MyArgsWithNonStrings>().MaybeDateTime == new DateTime(2016, 02, 29));
		}

		class MyRequiredArgs
		{
			[Required]
			public string ImportantField;
			[Required]
			public string ImportantProp { get; set; }
		}

		public void can_check_for_required_fields() {
			var e = Check.Exception<PowerArgsValidationException>(() => PowerArgs.Validate(new MyRequiredArgs()));
			Check.That(() => e.Errors.Count == 2);
		}
	}
}

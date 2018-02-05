using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using Cone;

namespace DataBoss.Specs
{
	[Describe(typeof(PowerArgs))]
	public class PowerArgsSpec
	{
		[Context("into parsing")]
		public class PowerArgsIntoSpec
		{
			class MySimpleArgs
			{
				public int Int;
				public bool Bool;
				public float Float;
			}

			public void reports_Parse_errors() {
				var e = Check.Exception<PowerArgsParseException>(() => PowerArgs
					.Parse("-Int", "Hello", "-Bool", "World", "-Float", "3,14")
					.Into<MySimpleArgs>());
				Check.That(() => e.Errors.Count == 2);
			}

			public class MyArg<T> { public T Value; }

			public void DateTime_parsing() => Check.That(
				() => PowerArgs.Parse("-Value", "2016-02-29").Into<MyArg<DateTime>>().Value == new DateTime(2016, 02, 29));

			public void reports_Enum_Parse_error() {
				var e = Check.Exception<PowerArgsParseException>(() => PowerArgs
					.Parse("-Value", "NoSuchValue")
					.Into<MyArg<MyEnum>>());
				Check.That(
					() => e.Errors.Count == 1,
					() => e.Errors[0].ArgumentName == "Value",
					() => e.Errors[0].Input == "NoSuchValue",
					() => e.Errors[0].ArgumentType == typeof(MyEnum));
			}

			public void reports_list_item_parse_errors() {
				var e = Check.Exception<PowerArgsParseException>(() => PowerArgs
					.Parse("-Value", "1,Foo,2,Bar")
					.Into<MyArg<List<int>>>());
				Check.That(
					() => e.Errors.Count == 2,
					() => e.Errors[0].ArgumentName == "Value",
					() => e.Errors[0].Input == "Foo",
					() => e.Errors[1].ArgumentName == "Value",
					() => e.Errors[1].Input == "Bar");
			}
		}

		public void uses_key_value_pairs() {
			var args = PowerArgs.Parse("-Foo", "Bar");
			Check.That(
				() => args.Count == 1, 
				() => args["Foo"] == "Bar");
		}

		public void merges_comma_separated_values_to_one()
		{
			var args = PowerArgs.Parse(
				"-A", "A,", "B",
				"-B", "A", ",B",
				"-C", "A", ",", "B",
				"-D", "A,", "B,", "C"
			);
			Check.That(
				() => args.Count == 4, 
				() => args["A"] == "A,B",
				() => args["B"] == "A,B",
				() => args["C"] == "A,B",
				() => args["D"] == "A,B,C");
		}

		public void missing_args_yields_InvalidOperationException() {
			Check.Exception<InvalidOperationException>(() => PowerArgs.Parse("-Foo"));
		}

		public void captures_non_options() {
			Check.With(() => PowerArgs.Parse("Hello", "World"))
			.That(
				args => args.Commands.Count == 2, 
				args => args.Commands[0] == "Hello",
				args => args.Commands[1] == "World");
		}

		public void captures_numbers_as_options()
		{
			Check.With(() => PowerArgs.Parse("-Foo", "Bar", "1", "-1"))
			.That(
				args => args.Commands.Count == 2,
				args => args.Commands[0] == "1",
				args => args.Commands[1] == "-1");

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

		#pragma warning disable CS0649
		class MyArgsWithDefaults
		{
			[DefaultValue("42")]
			public string TheAnswer;
		}
		#pragma warning restore CS0649

		public void uses_defaults_if_not_specified() {
			Check.That(() => PowerArgs.Parse().Into<MyArgsWithDefaults>().TheAnswer == "42");
		}

		#pragma warning disable CS0649
		class MyArgsWithNonStrings
		{
			public IEnumerable<float> NonStringable; 
			public List<int> MyList;
			public int MyInt;
			public DateTime MyDateTime;
			public DateTime? MaybeDateTime;
		}
		#pragma warning restore CS0649

		public void ignores_non_stringable_members() => Check.That(
				() => PowerArgs.Parse("-NonStringable", "42").Into<MyArgsWithNonStrings>().MyList == null);

		public void fills_list_like_with_members() => 
			Check.That(
				() => PowerArgs.Parse("-MyList", "1,2,3").Into<MyArgsWithNonStrings>().MyList.SequenceEqual(new [] { 1, 2, 3 }));

		public void parses_nullables() {
			Check.That(
				() => PowerArgs.Parse("-MaybeDateTime", "2016-02-29").Into<MyArgsWithNonStrings>().MaybeDateTime == new DateTime(2016, 02, 29));
		}

		#pragma warning disable CS0649
		class RequiredPropAndField
		{
			[Required]
			public string ImportantField;
			[Required]
			public string ImportantProp { get; set; }
		}
		#pragma warning restore CS0649

		public void can_check_for_required_fields() {
			var e = Check.Exception<PowerArgsValidationException>(() => PowerArgs.Validate(new RequiredPropAndField()));
			Check.That(() => e.Errors.Count == 2);
		}

		#pragma warning disable CS0649
		class MyArgsWithFlags
		{
			public bool MyFlag;
		}
		#pragma warning restore CS0649

		public void flags() {
			Check.That(
				() => PowerArgs.Parse("-MyFlag", "true").Into<MyArgsWithFlags>().MyFlag == true);
		}

		public void numbers_are_not_parameter_names() {
			var args = PowerArgs.Parse("-Value", "-1");
			Check.That(
				() => args.Count == 1,
				() => args["Value"] == "-1");
		}

		enum MyEnum
		{
			Nothing,
			Something
		}

		#pragma warning disable CS0649
		class MyArgsWithEnum
		{
			public MyEnum A;
			public MyEnum B;
			[DefaultValue(MyEnum.Something)]
			public MyEnum C;
			public MyEnum FromInt;
		}
		#pragma warning restore CS0649

		public void enums() => Check
			.With(() => PowerArgs.Parse("-A", "Nothing", "-B", "Something", "-FromInt", $"{(int)MyEnum.Something}"))
			.That(
				args => args.Count == 3,
				args => args.Into<MyArgsWithEnum>().A == MyEnum.Nothing,
				args => args.Into<MyArgsWithEnum>().B == MyEnum.Something,
				args => args.Into<MyArgsWithEnum>().C == MyEnum.Something,
				args => args.Into<MyArgsWithEnum>().FromInt == MyEnum.Something
			);

		public void describe_contains_props_and_fields() {
			var args = PowerArgs.Describe(typeof(RequiredPropAndField));
			Check.That(
				() => args.Any(x => x.Name == nameof(RequiredPropAndField.ImportantField)),
				() => args.Any(x => x.Name == nameof(RequiredPropAndField.ImportantProp)));
		}

		#pragma warning disable CS0649
		class MyOrderedArgs
		{
			[PowerArg(Order = 3)]
			public int Third;
			[PowerArg(Order = 1)]
			public int First;
			[PowerArg(Order = 2)]
			public int Second;

			public int NotOrdered;
		}
		#pragma warning restore CS0649

		public void describe_obeys_order() {
			var args = PowerArgs.Describe(typeof(MyOrderedArgs));

			Check.That(
				() => args[0].Name == "First",
				() => args[1].Name == "Second",
				() => args[2].Name == "Third",
				() => args[3].Name == "NotOrdered");
		}
	}
}

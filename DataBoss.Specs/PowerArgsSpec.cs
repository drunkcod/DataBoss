using System;
using Cone;
using System.Data;

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
	}
}

using System;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using Cone;
using Cone.Core;
using DataBoss.Migrations;

namespace DataBoss.Specs
{
	[Describe(typeof(ObjectReader))]
	public class ObjectReaderSpec
	{
		public void converts_all_rows() {
			var source = new SimpleDataReader("Id", "Context", "Name") {
				{ 1L, "", "First" },
				{ 2L, "", "Second" }
			};
			Check.That(() => ObjectReader.For(source).Read<DataBossMigrationInfo>().Count() == source.Count);
		}

		public void works_given_interface_reference() {
			var source = new SimpleDataReader("Id", "Context", "Name") {
				{ 1L, "", "First" },
				{ 2L, "", "Second" }
			};
			Check.That(() => ObjectReader.For(source as IDataReader).Read<DataBossMigrationInfo>().Count() == source.Count);
		}

		public void reads_public_fields() {
			var source = new SimpleDataReader("Id", "Context", "Name") { { 1L, "", "First" } };
			var read = ObjectReader.For(source).Read<DataBossMigrationInfo>().Single();
			Check.That(
				() => read.Id == 1,
				() => read.Context == "",
				() => read.Name == "First");
		}

		public void converter_fills_public_fields() {
			var source = new SimpleDataReader("Id", "Context", "Name");
			var formatter = new ExpressionFormatter(GetType());
			Check.That(() => formatter.Format(ObjectReader.MakeConverter<SimpleDataReader, DataBossMigrationInfo>(source)) == "x => new DataBossMigrationInfo { Id = x.GetInt64(0), Context = x.IsDBNull(1) ? default(string) : x.GetString(1), Name = x.IsDBNull(2) ? default(string) : x.GetString(2) }");
		}

		class ValueRow<T> { public T Value; }

		[Row(typeof(float), 3.14f)
		,Row(typeof(double), 42.17)
		,Row(typeof(int), int.MaxValue)
		,Row(typeof(short), short.MaxValue)
		,Row(typeof(byte), byte.MaxValue)
		, DisplayAs("{0}", Heading = "supports field of type")]
		public void supports_field_of_type(Type type, object value) {
			var check = (Action<object>)Delegate.CreateDelegate(typeof(Action<object>), GetType().GetMethod("CheckTSupport", BindingFlags.Static | BindingFlags.NonPublic).MakeGenericMethod(type));
			check(value);
		}

		static void CheckTSupport<T>(object value) {
			var source = new SimpleDataReader("Value");
			var expected = new ValueRow<T> { Value = (T)value };
			source.Add(expected.Value);
			Check.With(() => ObjectReader.For(source).Read<ValueRow<T>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Equals(expected.Value));
		}

		public void supports_binary_field() {
			var expected = new ValueRow<byte[]> { Value = Encoding.UTF8.GetBytes("Hello World!") };
			var source = new SimpleDataReader("Value") { expected.Value };
			Check.With(() => ObjectReader.For(source).Read<ValueRow<byte[]>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		public void supports_nested_fields() {
			var expected = new ValueRow<ValueRow<int>> { Value = new ValueRow<int> { Value = 42 } };
			var source = new SimpleDataReader("Value.Value") { expected.Value.Value };
			Check.With(() => ObjectReader.For(source).Read<ValueRow<ValueRow<int>>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Value == expected.Value.Value);
		}

		public void supports_deeply_nested_fields() {
			var expected = new ValueRow<ValueRow<ValueRow<int>>> { Value = new ValueRow<ValueRow<int>> { Value = new ValueRow<int> { Value = 42 } } };
			var source = new SimpleDataReader("Value.Value.Value") { expected.Value.Value.Value };
			Check.With(() => ObjectReader.For(source).Read<ValueRow<ValueRow<ValueRow<int>>>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Value.Value == expected.Value.Value.Value
			);
		}

		public void can_read_nullable_field() {
			var source = new SimpleDataReader("Value") {
				3.14f,
				new object[] { null }
			};
			Check.With(() => ObjectReader.For(source).Read<ValueRow<float?>>())
			.That(
				rows => rows.First().Value == 3.14f,
				rows => rows.Last().Value == null);
		}

		struct StructRow<T> { public T Value; }

		public void can_read_structs() {
			var expected = new StructRow<float> { Value = 3.14f };
			var source = new SimpleDataReader("Value") { expected.Value };
			Check.With(() => ObjectReader.For(source).Read<StructRow<float>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		class MyThing<T>
		{
			public MyThing(T value) { this.Value = value; }
			public T Value { get; }
		}

		public void can_use_parameterized_ctor() {
			var expected = new MyThing<int>(42);
			var source = new SimpleDataReader("value") { expected.Value };
			Check.With(() => ObjectReader.For(source).Read<MyThing<int>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		public void ctors_can_have_complex_arguments() {
			var expected = new MyThing<MyThing<int>> (new MyThing<int>(42));
			var source = new SimpleDataReader("value.value") { expected.Value.Value };
			Check.With(() => ObjectReader.For(source).Read<MyThing<MyThing<int>>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Value == expected.Value.Value);
		}

		public void Action_support() {
			var expected = new MyThing<int>(42);
			var source = new SimpleDataReader("value") { expected.Value };
			MyThing<int> actual = null;
			ObjectReader.For(source).Read((MyThing<int> x) => actual = x);
			Check.That(() => actual != null);
			Check.That(() => actual.Value == expected.Value); 
		}
	}
}
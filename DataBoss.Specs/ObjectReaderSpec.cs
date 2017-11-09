using Cone;
using Cone.Core;
using DataBoss.Data;
using DataBoss.Migrations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;

namespace DataBoss.Specs
{
	[Describe(typeof(ObjectReader))]
	public class ObjectReaderSpec
	{
		static KeyValuePair<string, Type> Col<T>(string name) => new KeyValuePair<string, Type>(name, typeof(T));

		public void converts_all_rows() {
			var source = new SimpleDataReader(Col<long>("Id"), Col<string>("Context"), Col<string>("Name")) {
				{ 1L, "", "First" },
				{ 2L, "", "Second" }
			};
			Check.That(() => ObjectReader.For(source).Read<DataBossMigrationInfo>().Count() == source.Count);
		}

		public void works_given_interface_reference() {
			var source = new SimpleDataReader(Col<long>("Id"), Col<string>("Context"), Col<string>("Name")) {
				{ 1L, "", "First" },
				{ 2L, "", "Second" }
			};
			Check.That(() => ObjectReader.For(source as IDataReader).Read<DataBossMigrationInfo>().Count() == source.Count);
		}

		public void reads_public_fields() {
			var source = new SimpleDataReader(Col<long>("Id"), Col<string>("Context"), Col<string>("Name")) { { 1L, "", "First" } };
			var read = ObjectReader.For(source).Read<DataBossMigrationInfo>().Single();
			Check.That(
				() => read.Id == 1,
				() => read.Context == "",
				() => read.Name == "First");
		}

		public void converter_fills_public_fields() {
			var source = new SimpleDataReader(Col<long>("Id"), Col<string>("Context"), Col<string>("Name"));
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
			var source = new SimpleDataReader(Col<T>("Value"));
			var expected = new ValueRow<T> { Value = (T)value };
			source.Add(expected.Value);
			Check.With(() => ObjectReader.For(source).Read<ValueRow<T>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Equals(expected.Value));
		}

		public void supports_binary_field() {
			var expected = new ValueRow<byte[]> { Value = Encoding.UTF8.GetBytes("Hello World!") };
			var source = new SimpleDataReader(Col<byte[]>("Value")) { expected.Value };
			Check.With(() => ObjectReader.For(source).Read<ValueRow<byte[]>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		public void supports_nested_fields() {
			var expected = new ValueRow<ValueRow<int>> { Value = new ValueRow<int> { Value = 42 } };
			var source = new SimpleDataReader(Col<int>("Value.Value")) { expected.Value.Value };
			Check.With(() => ObjectReader.For(source).Read<ValueRow<ValueRow<int>>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Value == expected.Value.Value);
		}

		public void supports_deeply_nested_fields() {
			var expected = new ValueRow<ValueRow<ValueRow<int>>> { Value = new ValueRow<ValueRow<int>> { Value = new ValueRow<int> { Value = 42 } } };
			var source = new SimpleDataReader(Col<int>("Value.Value.Value")) { expected.Value.Value.Value };
			Check.With(() => ObjectReader.For(source).Read<ValueRow<ValueRow<ValueRow<int>>>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Value.Value == expected.Value.Value.Value
			);
		}

		public void can_read_nullable_field() {
			var source = new SimpleDataReader(Col<float>("Value.Value")) {
				3.14f,
			};
			Check.With(() => ObjectReader.For(source).Read<ValueRow<StructRow<float>>>())
			.That(rows => rows.First().Value.Value == 3.14f);
		}

		struct StructRow<T> { public T Value; }

		public void can_read_structs() {
			var expected = new StructRow<float> { Value = 3.14f };
			var source = new SimpleDataReader(Col<float>("Value")) { expected.Value };
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
			var source = new SimpleDataReader(Col<int>("value")) { expected.Value };
			Check.With(() => ObjectReader.For(source).Read<MyThing<int>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		public void ctors_can_have_complex_arguments() {
			var expected = new MyThing<MyThing<int>> (new MyThing<int>(42));
			var source = new SimpleDataReader(Col<int>("value.value")) { expected.Value.Value };
			Check.With(() => ObjectReader.For(source).Read<MyThing<MyThing<int>>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Value == expected.Value.Value);
		}

		public void Action_support() {
			var expected = new MyThing<int>(42);
			var source = new SimpleDataReader(Col<int>("value")) { expected.Value };
			MyThing<int> actual = null;
			ObjectReader.For(source).Read((MyThing<int> x) => actual = x);
			Check.That(() => actual != null);
			Check.That(() => actual.Value == expected.Value); 
		}

		class ValueProp<T>
		{
			public T Value { get; set; }
		}

		public void can_read_props() {
			var expected = new ValueProp<float> { Value = 2.78f };
			var source = new SimpleDataReader(Col<float>("Value")) { expected.Value };
			Check.With(() => ObjectReader.For(source).Read<ValueProp<float>>().ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		public void detect_type_mismatch() {
			var expected = new ValueProp<float> { Value = 2.78f };
			var source = new SimpleDataReader(Col<float>("Value")) { expected.Value };
			Check.Exception<InvalidOperationException>(() => ObjectReader.For(source).Read<ValueProp<int>>().Count());
		}

		public void custom_converter() {
			var expected = new ValueProp<int> { Value = 42 };
			var source = new SimpleDataReader(Col<int>("Value")) { expected.Value };
			var reader = ObjectReader
				.For(source)
				.WithConverter((int x) => x.ToString());
			Check
				.With(() => reader.Read<ValueProp<string>>().ToArray())
				.That(x => x[0].Value == "42");
		}

		public void custom_converter_nullable_handling() {
			var expected = new ValueProp<int?> { Value = 12345 };
			var source = new SimpleDataReader(Col<int>("Value")) { expected.Value };
			var reader = ObjectReader
				.For(source)
				.WithConverter((int x) => (long)x);
			Check
				.With(() => reader.Read<ValueProp<long?>>().ToArray())
				.That(x => x[0].Value == 12345);
		}
	}
}
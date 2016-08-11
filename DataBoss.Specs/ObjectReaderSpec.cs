using System;
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
		ObjectReader ObjectReader;

		[BeforeEach] 
		public void given_a_new_ObjectReader() {
			this.ObjectReader = new ObjectReader();
		}

		public void converts_all_rows() {
			var source = new SimpleDataReader("Id", "Context", "Name") {
				{ 1L, "", "First" },
				{ 2L, "", "Second" }
			};
			Check.That(() => ObjectReader.Read<DataBossMigrationInfo>(source).Count() == source.Count);
		}

		public void reads_public_fields() {
			var source = new SimpleDataReader("Id", "Context", "Name") { { 1L, "", "First" } };
			var read = ObjectReader.Read<DataBossMigrationInfo>(source).Single();
			Check.That(
				() => read.Id == 1,
				() => read.Context == "",
				() => read.Name == "First");
		}

		public void converter_fills_public_fields() {
			var source = new SimpleDataReader("Id", "Context", "Name");
			var formatter = new ExpressionFormatter(GetType());
			Check.That(() => formatter.Format(ObjectReader.MakeConverter<DataBossMigrationInfo>(source)) == "x => new DataBossMigrationInfo { Id = x.GetInt64(0), Context = x.IsDBNull(1) ? default(string) : x.GetString(1), Name = x.IsDBNull(2) ? default(string) : x.GetString(2) }");
		}

		class ValueRow<T> { public T Value; }

		[Row(typeof(float), 3.14f)
		,Row(typeof(double), 42.17)
		,Row(typeof(int), 1)
		,Row(typeof(short), (short)2)
		,DisplayAs("{0}", Heading = "supports field of type")]
		public void supports_field_of_type(Type type, object value) {
			var check = (Action<ObjectReader,object>)Delegate.CreateDelegate(typeof(Action<ObjectReader,object>), GetType().GetMethod("CheckTSupport", BindingFlags.Static | BindingFlags.NonPublic).MakeGenericMethod(type));
			check(ObjectReader, value);
		}

		static void CheckTSupport<T>(ObjectReader reader, object value) {
			var source = new SimpleDataReader("Value");
			var expected = new ValueRow<T> { Value = (T)value };
			source.Add(expected.Value);
			Check.With(() => reader.Read<ValueRow<T>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Equals(expected.Value));
		}

		public void supports_binary_field() {
			var expected = new ValueRow<byte[]> { Value = Encoding.UTF8.GetBytes("Hello World!") };
			var source = new SimpleDataReader("Value") { expected.Value };
			Check.With(() => ObjectReader.Read<ValueRow<byte[]>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		public void supports_nested_fields() {
			var expected = new ValueRow<ValueRow<int>> { Value = new ValueRow<int> { Value = 42 } };
			var source = new SimpleDataReader("Value.Value") { expected.Value.Value };
			Check.With(() => ObjectReader.Read<ValueRow<ValueRow<int>>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Value == expected.Value.Value);
		}

		public void supports_deeply_nested_fields() {
			var expected = new ValueRow<ValueRow<ValueRow<int>>> { Value = new ValueRow<ValueRow<int>> { Value = new ValueRow<int> { Value = 42 } } };
			var source = new SimpleDataReader("Value.Value.Value") { expected.Value.Value.Value };
			Check.With(() => ObjectReader.Read<ValueRow<ValueRow<ValueRow<int>>>>(source).ToArray())
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
			Check.With(() => ObjectReader.Read<ValueRow<float?>>(source))
			.That(
				rows => rows.First().Value == 3.14f,
				rows => rows.Last().Value == null);
		}

		struct StructRow<T> { public T Value; }

		public void can_read_structs() {
			var expected = new StructRow<float> { Value = 3.14f };
			var source = new SimpleDataReader("Value") { expected.Value };
			Check.With(() => ObjectReader.Read<StructRow<float>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		class MyThing
		{
			public MyThing(int value) { this.Value = value; }
			public int Value { get; }
		}

		public void can_use_parameterized_ctor() {
			var expected = new MyThing(42);
			var source = new SimpleDataReader("value") { expected.Value };
			Check.With(() => ObjectReader.Read<MyThing>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}
	}
}
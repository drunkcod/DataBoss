using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using CheckThat;
using CheckThat.Formatting;
using DataBoss.Data.SqlServer;
using DataBoss.Linq;
using DataBoss.Migrations;
using DataBoss.Specs;
using Xunit;

namespace DataBoss.Data
{
	public class ObjectReaderSpec
	{
		static KeyValuePair<string, Type> Col<T>(string name) => new KeyValuePair<string, Type>(name, typeof(T));

		[Fact]
		public void converts_all_rows() {
			var source = new SimpleDataReader(Col<long>("Id"), Col<string>("Context"), Col<string>("Name")) {
				{ 1L, "", "First" },
				{ 2L, "", "Second" }
			};
			Check.That(() => ObjectReader.Read<DataBossMigrationInfo>(source).Count() == source.Count);
		}

		[Fact]
		public void works_given_interface_reference() {
			var source = new SimpleDataReader(Col<long>("Id"), Col<string>("Context"), Col<string>("Name")) {
				{ 1L, "", "First" },
				{ 2L, "", "Second" }
			};
			Check.That(() => ObjectReader.Read<DataBossMigrationInfo>(source as IDataReader).Count() == source.Count);
		}

		[Fact]
		public void reads_public_fields() {
			var source = new SimpleDataReader(Col<long>("Id"), Col<string>("Context"), Col<string>("Name")) { { 1L, "", "First" } };
			var read = ObjectReader.Read<DataBossMigrationInfo>(source).Single();
			Check.That(
				() => read.Id == 1,
				() => read.Context == "",
				() => read.Name == "First");
		}

		[Fact]
		public void converter_fills_public_fields() {
			var source = SequenceDataReader.Create(new[] { new DataBossMigrationInfo() }, x => { 
				x.Map("Id");
				x.Map("Context");
				x.Map("Name");
			});
			var formatter = new ExpressionFormatter(GetType());
			Check.That(() => formatter.Format(ObjectReader.MakeConverter<IDataReader, DataBossMigrationInfo>(source)) == 
			"x => new DataBossMigrationInfo { Id = x.GetInt64(0), Context = x.IsDBNull(1) ? default(string) : x.GetString(1), Name = x.IsDBNull(2) ? default(string) : x.GetString(2) }");
		}

		class ValueRow<T> { public T Value; }

		[Theory]
		[InlineData(typeof(float), 3.14f)]
		[InlineData(typeof(double), 42.17)]
		[InlineData(typeof(int), int.MaxValue)]
		[InlineData(typeof(short), short.MaxValue)]
		[InlineData(typeof(byte), byte.MaxValue)]
		public void supports_field_of_type(Type type, object value) {
			var check = (Action<object>)Delegate.CreateDelegate(typeof(Action<object>), GetType().GetMethod("CheckTSupport", BindingFlags.Static | BindingFlags.NonPublic).MakeGenericMethod(type));
			check(value);
		}

		static void CheckTSupport<T>(object value) {
			var source = new SimpleDataReader(Col<T>("Value"));
			var expected = new ValueRow<T> { Value = (T)value };
			source.Add(expected.Value);
			Check.With(() => ObjectReader.Read<ValueRow<T>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Equals(expected.Value));
		}

		[Fact]
		public void supports_binary_field() {
			var expected = new ValueRow<byte[]> { Value = Encoding.UTF8.GetBytes("Hello World!") };
			var source = new SimpleDataReader(Col<byte[]>("Value")) { expected.Value };
			Check.With(() => ObjectReader.Read<ValueRow<byte[]>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		[Fact]
		public void supports_timespan() {
			var expected = new ValueRow<TimeSpan> { Value = TimeSpan.FromSeconds(42) };
			var source = new SimpleDataReader(Col<TimeSpan>("Value")) { expected.Value };
			Check.With(() => ObjectReader.Read<ValueRow<TimeSpan>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		enum MyEnum32 : int { Something = 1 }

		[Fact]
		public void supports_enums() {
			var source = new SimpleDataReader(Col<MyEnum32>("Value"));
			var expected = new ValueRow<MyEnum32> { Value = MyEnum32.Something };
			source.Add(expected.Value);
			Check.With(() => ObjectReader.Read<ValueRow<MyEnum32>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Equals(expected.Value));

		}

		[Fact]
		public void supports_nested_fields() {
			var expected = new ValueRow<ValueRow<int>> { Value = new ValueRow<int> { Value = 42 } };
			var source = new SimpleDataReader(Col<int>("Value.Value")) { expected.Value.Value };
			Check.With(() => ObjectReader.Read<ValueRow<ValueRow<int>>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Value == expected.Value.Value);
		}

		[Fact]
		public void supports_deeply_nested_fields() {
			var expected = new ValueRow<ValueRow<ValueRow<int>>> { Value = new ValueRow<ValueRow<int>> { Value = new ValueRow<int> { Value = 42 } } };
			var source = new SimpleDataReader(Col<int>("Value.Value.Value")) { expected.Value.Value.Value };
			Check.With(() => ObjectReader.Read<ValueRow<ValueRow<ValueRow<int>>>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Value.Value == expected.Value.Value.Value
			);
		}

		[Fact]
		public void can_read_nullable_field() {
			var source = new SimpleDataReader(Col<float>("Value.Value")) {
				3.14f,
			};
			Check.With(() => ObjectReader.Read<ValueRow<StructRow<float>>>(source))
			.That(rows => rows.First().Value.Value == 3.14f);
		}

		[Fact]
		public void null_source_becomes_default_value() {
			var rows = ObjectReader.For(() => SequenceDataReader.Create(new[] { new StructRow<int?> { Value = null }, new StructRow<int?> { Value = 1 } }, x => x.MapAll()));
			Check.With(() => rows.Read<StructRow<int>>().ToArray())
				.That(xs => xs[0].Value == default(int), xs => xs[1].Value == 1);		
		}

		struct StructRow<T> 
		{
			public StructRow(T ctorValue) {  this.Value = ctorValue; this.ReadonlyValue = ctorValue; }
			public T Value; 
			public readonly T ReadonlyValue;
		}

		[Fact]
		public void can_read_structs() {
			var expected = new StructRow<float> { Value = 3.14f };
			var source = new SimpleDataReader(Col<float>("Value")) { expected.Value };
			Check.With(() => ObjectReader.Read<StructRow<float>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);	
		}

		[Fact]
		public void can_read_nullable_struct() {
			var expected = 3.14f;
			var source = new SimpleDataReader(Col<float>("Value.ctorValue")) { expected };
			Check.With(() => ObjectReader.Read<StructRow<StructRow<float>?>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Value.Value == expected);
		}

		[Fact]
		public void null_nullable_struct() {
			var source = new SimpleDataReader(Col<float>("Value.ctorValue")) { new object[]{ null } };
			source.SetNullable(0, true);
			var r = ObjectReader.Read<StructRow<StructRow<float>?>>(source).ToArray();
			Check.With(() => r)
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.HasValue == false);
		}

		#pragma warning disable CS0649
		struct WithNullable
		{
			public int? CanBeNull;
			public int NotNull; 
		}
		#pragma warning restore CS0649

		[Fact]
		public void nullable_nullable() {
			var source = new SimpleDataReader(Col<int>("Value.CanBeNull"), Col<int>("Value.NotNull")) { new object[] { null, 1 } };
			source.SetNullable(0, true);
			var r = ObjectReader.Read<StructRow<WithNullable?>>(source).ToArray();
			Check.With(() => r)
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Equals(new WithNullable { NotNull = 1 }));
		}

		#pragma warning disable CS0649
		struct RowOf<T>
		{
			public T Item;
		}
		#pragma warning restore CS0649

		[Fact]
		public void row_with_nullable_missing_field() {
			var source = new SimpleDataReader(Col<int>("Item.ctorValue")) { new object[] { null } };
			source.SetNullable(0, true);
			var r = ObjectReader.Read<RowOf<StructRow<int>?>>(source).ToArray();
			Check.With(() => r)
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Item.HasValue == false);

		}

		[Fact]
		public void row_with_nullab_missing_field2() {
			var source = new SimpleDataReader(Col<int>("Item.key"), Col<int>("Item.value")) { new object[] { null, null} };
			source.SetNullable(0, true);
			source.SetNullable(1, true);
			var r = ObjectReader.Read<RowOf<KeyValuePair<int,int?>?>>(source).ToArray();
			Check.With(() => r)
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Item.HasValue == false);
		}

		class MyThing<T>
		{
			public MyThing(T value) { this.Value = value; }
			public T Value { get; }
		}

		[Fact]
		public void can_use_parameterized_ctor() {
			var expected = new MyThing<int>(42);
			var source = new SimpleDataReader(Col<int>("value")) { expected.Value };
			Check.With(() => ObjectReader.Read<MyThing<int>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		[Fact]
		public void ctors_can_have_complex_arguments() {
			var expected = new MyThing<MyThing<int>> (new MyThing<int>(42));
			var source = new SimpleDataReader(Col<int>("value.value")) { expected.Value.Value };
			Check.With(() => ObjectReader.Read<MyThing<MyThing<int>>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value.Value == expected.Value.Value);
		}

		[Fact]
		public void Action_support() {
			var expected = new MyThing<int>(42);
			var source = new SimpleDataReader(Col<int>("value")) { expected.Value };
			MyThing<int> actual = null;
			ObjectReader.Read(source, (MyThing<int> x) => actual = x);
			Check.That(() => actual != null);
			Check.That(() => actual.Value == expected.Value); 
		}

		class ValueProp<T>
		{
			public T Value { get; set; }
		}

		[Fact]
		public void can_read_props() {
			var expected = new ValueProp<float> { Value = 2.78f };
			var source = new SimpleDataReader(Col<float>("Value")) { expected.Value };
			Check.With(() => ObjectReader.Read<ValueProp<float>>(source).ToArray())
			.That(
				rows => rows.Length == 1,
				rows => rows[0].Value == expected.Value);
		}

		[Fact]
		public void provides_type_context_on_type_missmatch() {
			var expected = new ValueProp<float> { Value = 2.78f };
			var source = new SimpleDataReader(Col<int>("Value")) { expected.Value };
			var ex = Check.Exception<InvalidConversionException>(() => ObjectReader.Read<ValueProp<float>>(source).ToArray());
			Check.That(() => ex.Type == typeof(ValueProp<float>));
		}

		[Fact]
		public void detect_type_mismatch() {
			var expected = new ValueProp<float> { Value = 2.78f };
			var source = new SimpleDataReader(Col<float>("Value")) { expected.Value };
			Check.Exception<InvalidOperationException>(() => ObjectReader.Read<ValueProp<int>>(source).Count());
		}

		[Fact]
		public void custom_converter() {
			var expected = new ValueProp<int> { Value = 42 };
			var reader = ObjectReader
				.For(() => new SimpleDataReader(Col<int>("Value")) { expected.Value })
				.WithConverter((int x) => x.ToString());
			Check
				.With(() => reader.Read<ValueProp<string>>().ToArray())
				.That(x => x[0].Value == "42");
		}

		[Fact]
		public void custom_converter_nullable_handling() {
			var expected = new ValueProp<int?> { Value = 12345 };
			var reader = ObjectReader
				.For(() => new SimpleDataReader(Col<int>("Value")) { expected.Value })
				.WithConverter((int x) => (long)x);
			Check
				.With(() => reader.Read<ValueProp<long?>>().ToArray())
				.That(x => x[0].Value == 12345);
		}

		[Fact]
		public void custom_converter_null_item_handling() {
			var expected = new ValueProp<int?> { Value = 12345 };
			var reader = ObjectReader
				.For(() => new SimpleDataReader(Col<int>("Value")) { expected.Value })
				.WithConverter((int x) => (long)x);
			Check
				.With(() => reader.Read<ValueProp<long?>>().ToArray())
				.That(x => x[0].Value == 12345);
		}

		[Fact]
		public void doesnt_attempt_to_set_readonly_fields() {
			var source = new SimpleDataReader(
				Col<int>("Value"),
				Col<int>("ReadonlyValue"));
			source.Add(0, 1);
			Check
				.With(() => ObjectReader.Read<StructRow<int>>(source).ToArray())
				.That(x => x[0].Value == 0);
		}

		#pragma warning disable CS0649
		class MyRequiredValue<T>
		{
			[Required]
			public T Value;
		}
		#pragma warning restore CS0649


		[Fact]
		public void ensures_required_fields_are_present() {
			var source = new SimpleDataReader(Col<int>($"Not{nameof(MyRequiredValue<int>.Value)}"));
			source.Add(1);
			Check.Exception<ArgumentException>(() => ObjectReader.Read<MyRequiredValue<int>>(source).ToArray());
		}

		[Fact]
		public void IdOf() {
			var source = new SimpleDataReader(Col<int>("Value"));
			source.Add(1);
			Check
				.With(() => ObjectReader.Read<ValueRow<IdOf<ValueRow<int>>>>(source).ToArray())
				.That(x => x[0].Value == new IdOf<ValueRow<int>>(1));
		}

		[Fact]
		public void RowVersion_from_bytes() {
			var source = new SimpleDataReader(Col<byte[]>("Value.value"));
			source.Add(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8 });
			Check
				.With(() => ObjectReader.Read<ValueRow<RowVersion>>(source).ToArray())
				.That(x => x[0].Value == new RowVersion(new byte[]{ 1, 2, 3, 4, 5, 6, 7, 8, }));
		}

		class MyCastable
		{
			public int Value;

			public static implicit operator MyCastable(int value) => new MyCastable { Value = value };
			public static explicit operator MyCastable(long value) => new MyCastable { Value = (int)value };
		}

		[Fact]
		public void implicit_cast() {
			var source = new SimpleDataReader(Col<int>("Value"));
			source.Add(10);
			Check
				.With(() => ObjectReader.Read<MyRequiredValue<MyCastable>>(source).ToArray())
				.That(x => x[0].Value.Value == 10);
		}

		[Fact]
		public void explicit_cast() {
			var source = new SimpleDataReader(Col<long>("Value"));
			source.Add(10L);
			Check
				.With(() => ObjectReader.Read<MyRequiredValue<MyCastable>>(source).ToArray())
				.That(x => x[0].Value.Value == 10);
		}
	}
}
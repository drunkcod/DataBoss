using System;
using System.Linq;
using System.Linq.Expressions;
using CheckThat;
using Xunit;

namespace DataBoss.Data
{
	public class FieldMappingSpec
	{
		class MyThing
		{
			public int Field;
			public double Prop { get; set; }
		}

		[Fact]
		public void untyped_lambda_mapping_must_have_correct_parameter_type() {
			var fieldMapping = new FieldMapping<MyThing>();
			Check.Exception<InvalidOperationException>(() => fieldMapping.Map("Borken", MakeLambda((string x) => x)));
		}

		[Fact]
		public void untyped_lambda_mapping() {
			var fieldMapping = new FieldMapping<MyThing>();
			fieldMapping.Map("LambdaValue", MakeLambda((MyThing x) => x.Field));
			var accessor = fieldMapping.GetAccessor();
			var result = new object[1];
			accessor(new MyThing { Field = 1 }, result);
			Check.That(() => (int)result[0] == 1);
		}

		[Fact]
		public void lambdas_not_wrapped_uncessarily() {
			var fieldMapping = new FieldMapping<MyThing>();
			Func<MyThing, int> failToGetValue = x => { throw new InvalidOperationException(); };
			fieldMapping.Map("Borken", MakeLambda((MyThing x) => failToGetValue(x)));

			Check.That(() => fieldMapping.GetAccessorExpression().Body.ToString().StartsWith("(target[0] = Convert(Invoke(value("));
		}

		[Fact]
		public void static_member_lambda_mapping() {
			var fieldMapping = new FieldMapping<MyThing>();
			fieldMapping.Map("Empty", MakeLambda((MyThing x) => string.Empty));

			Check.That(() => fieldMapping.GetAccessorExpression().Body.ToString() == "(target[0] = String.Empty)");
		}

		[Fact]
		public void nullable() {
			var nullItem = new { Value = (int?)null };
			var item1 = new { Value = (int?)1 };
			var fieldMapping = new FieldMapping(nullItem.GetType());
			fieldMapping.MapAll();

			Check.That(
				() => fieldMapping.GetSelector(0).ToString() == "IIF(source.Value.HasValue, Convert(source.Value.Value, Object), )",
				() => fieldMapping.Invoke(nullItem).Single() == DBNull.Value,
				() => fieldMapping.Invoke(item1).Single() == (object)item1.Value);
		}

		[Fact]
		public void primitive_types_selector_retains_type() {
			var fieldMapping = new FieldMapping<MyThing>();
			var field = fieldMapping.Map(x => x.Field);
			var prop = fieldMapping.Map(x => x.Prop);
			var str = fieldMapping.Map("Hello", x => "Hello");

			Check.That(
				() => fieldMapping.GetSelector(field).Type == typeof(int),
				() => fieldMapping.GetSelector(prop).Type == typeof(double),
				() => fieldMapping.GetSelector(str).Type == typeof(string));
		}

		#pragma warning disable CS0649
		class MyThingWithStaticMember
		{
			public string TheAnswer;
			public static string TheQuestion;
			public static float YourBoat => 42;
		}
		#pragma warning restore CS0649

		[Fact]
		public static void MapAll_ignores_static_fields() {
			var mapping = new FieldMapping<MyThingWithStaticMember>();

			mapping.MapAll();

			Check.That(() => mapping.Count == 1);
		}

		LambdaExpression MakeLambda<TArg, TResult>(Expression<Func<TArg, TResult>> expr) => expr;
	}
}

using Cone;
using System;
using System.Linq.Expressions;

namespace DataBoss.Data
{
	[Describe(typeof(FieldMapping<>))]
	public class FieldMappingSpec
	{
		class MyThing
		{
			public int Value;
		}

		public void untyped_lambda_mapping_must_have_correct_parameter_type() {
			var fieldMapping = new FieldMapping<MyThing>();
			Check.Exception<InvalidOperationException>(() => fieldMapping.Map("Borken", MakeLambda((string x) => x)));
		}

		public void untyped_lambda_mapping() {
			var fieldMapping = new FieldMapping<MyThing>();
			fieldMapping.Map("LambdaValue", MakeLambda((MyThing x) => x.Value));
			var accessor = fieldMapping.GetAccessor();
			var result = new object[1];
			accessor(new MyThing { Value = 1 }, result);
			Check.That(() => (int)result[0] == 1);
		}

		public void lambdas_not_wrapped_uncessarily() {
			var fieldMapping = new FieldMapping<MyThing>();
			Func<MyThing, int> failToGetValue = x => { throw new InvalidOperationException(); };
			fieldMapping.Map("Borken", MakeLambda((MyThing x) => failToGetValue(x)));

			Check.That(() => fieldMapping.GetAccessorExpression().Body.ToString().StartsWith("(target[0] = Convert(Invoke(value("));
		}

		public void static_member_lambda_mapping() {
			var fieldMapping = new FieldMapping<MyThing>();
			fieldMapping.Map("Empty", MakeLambda((MyThing x) => string.Empty));

			Check.That(() => fieldMapping.GetAccessorExpression().Body.ToString() == "(target[0] = Convert(String.Empty))");
		}

		class MyThingWithStaticMember
		{
			public string TheAnswer;
			public static string TheQuestion;
			public static float YourBoat => 42;
		}

		public static void MapAll_ignores_static_fields() {
			var mapping = new FieldMapping<MyThingWithStaticMember>();

			mapping.MapAll();

			Check.That(() => mapping.GetFieldNames().Length == 1);
		}

		LambdaExpression MakeLambda<TArg, TResult>(Expression<Func<TArg, TResult>> expr) => expr;
	}
}

using Cone;
using DataBoss.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

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
			fieldMapping.Map("Borken", MakeLambda((MyThing x) => x.Value));
			var accessor = fieldMapping.GetAccessor();
			var result = new object[1];
			accessor(new MyThing { Value = 1 }, result);
			Check.That(() => (int)result[0] == 1);
		}

		LambdaExpression MakeLambda<TArg, TResult>(Expression<Func<TArg, TResult>> expr) => expr;
	}
}

using Cone;
using System.Collections.Generic;

namespace DataBoss.Data.Specs
{
	[Describe(typeof(SqlQuery))]
	public class SqlQuerySpec
	{
		public void select_ctor() => Check
			.With(() => SqlQuery.Select(() => new KeyValuePair<int, string>(SqlQuery.Column<int>("TheTable", "Id"), SqlQuery.Column<string>("TheTable", "Text"))))
			.That(q => q.ToString() == "select [key] = TheTable.Id, [value] = TheTable.Text");

		public class MyRow<T> { public T Whatever; }

		public void select_member_init() => Check
			.With(() => SqlQuery.Select(() => new MyRow<float> { Whatever = SqlQuery.Column<float>("Your", "Boat") }))
			.That(q => q.ToString() == "select [Whatever] = Your.Boat");

		public void select_recursive_init() => Check
			.With(() => SqlQuery.Select(() => new MyRow<MyRow<int>> { Whatever = new MyRow<int> { Whatever = SqlQuery.Column<int>("Answer", "Value") } }))
			.That(q => q.ToString() == "select [Whatever.Whatever] = Answer.Value");

		public void select_recursive_ctor() => Check
			.With(() => SqlQuery.Select(() => new MyRow<KeyValuePair<int, int>> { Whatever = new KeyValuePair<int, int>(SqlQuery.Column<int>("Answer", "Key"), SqlQuery.Column<int>("Answer", "Value")) }))
			.That(q => q.ToString() == "select [Whatever.key] = Answer.Key, [Whatever.value] = Answer.Value");

		public void select_recursive_member_init() => Check
			.With(() => SqlQuery.Select(() => new KeyValuePair<int, MyRow<int>>(SqlQuery.Column<int>("Things", "Id"), new MyRow<int> { Whatever = SqlQuery.Column<int>("Answer", "Value") })))
			.That(q => q.ToString() == "select [key] = Things.Id, [value.Whatever] = Answer.Value");

		public void select_non_inline_method_call_column_definition()
		{
			var source = SqlQuery.Column<int>("Answers", "Value");
			Check.With(() => SqlQuery.Select(() => new { TheAnswer = source }))
				.That(q => q.ToString() == "select [TheAnswer] = Answers.Value");
		}

		public void select_null_column() => Check
			.With(() => SqlQuery.Select(() => new { Value = SqlQuery.Null<int?>() }))
			.That(q => q.ToString() == "select [Value] = cast(null as int)");

		public struct MyStruct { public int Value; }

		public void select_without_ctor() => Check
			.With(() => SqlQuery.Select(() => new MyStruct { Value = SqlQuery.Column<int>("MyTable", "Id") }))
			.That(q => q.ToString() == "select [Value] = MyTable.Id");

		public void select_formatting_indented() => Check
			.With(() => SqlQuery.Select(() => new {  Value = SqlQuery.Column<int>("Source", "Column")}))
			.That(q => q.ToString(SqlQueryFormatting.Indented) == "select\n\t[Value] = Source.Column\n");
	}
}

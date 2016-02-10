using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Cone;

namespace DataBoss.Specs
{
	public static class ObjectVisualizer
	{
		public static TextWriter Output = Console.Out;

		public static T Dump<T>(this T self) {
			Dump(self, typeof(T));
			return self;
		}

		static void Dump(object obj, Type type)
		{
			if(IsBasicType(type)) {
				Output.WriteLine(obj);
				return;
			}

			var xs = obj as IEnumerable;
			if(xs != null) {
				foreach(var item in xs)
					Dump(item, item.GetType());
			} else {
				foreach(var item in type.GetProperties(BindingFlags.Instance | BindingFlags.Public))
				{
					if(item.CanRead)
						$"{item.Name}: {item.GetValue(obj)}".Dump();
				}
			}
		}

		static bool IsBasicType(Type type) {
			switch(type.FullName) {
				case "System.String": return true;
				case "System.Int32": return true;
			}
			return false;
		}
	}

	[Describe(typeof(ObjectVisualizer))]
	public class ObjectVisualizerSpec
	{
		[Context("Dump")]
		public class ObjectVisualizer_Dump
		{
			StringWriter Output;

			[BeforeEach]
			public void ResetOutput() {
				Output = new StringWriter();
				ObjectVisualizer.Output = Output;
			}

			public void returns_self() {
				var self = "Hello World";
				Check.That(() => object.ReferenceEquals(self, self.Dump()));
			}

			public void puts_each_object_on_single_line() {
				"Hello World!".Dump();
				42.Dump();
				Check.That(() => Output.ToString() == "Hello World!\r\n42\r\n");
			}

			public void outputs_row_per_item_in_sequence() {
				new[] { 1, 2, 3 }.Dump();
				Check.That(() => Output.ToString() == "1\r\n2\r\n3\r\n");
			}

			public void displays_field_and_value_for_single_object() {
				new { Hello = "World" }.Dump();
				Check.That(() => Output.ToString() == "Hello: World\r\n");
			}
		}
	}
}

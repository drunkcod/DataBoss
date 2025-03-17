namespace DataBoss.Data.Debug
{
	using System;
	using System.Data;
	using System.IO;
	using System.Linq;
	using System.Linq.Expressions;
	using DataBoss.Linq;

	public static class ExpressionDebug
	{
		public static void WriteExpr(LambdaExpression fn, TextWriter writer) {
			int n = 0;
			string NextParam() => $"Param_{n++}";
			writer.WriteLine($"({string.Join(", ", fn.Parameters.Select(x => $"{x.Name ?? NextParam()}"))}) => ");
			WriteExpr(fn.Body, writer, "");
		}

		public static void WriteExpr(Expression expr, TextWriter writer, string indent) {
			switch (expr.NodeType) {
				case ExpressionType.Assign:
					var assign = (BinaryExpression)expr;
					var left = new StringWriter();
					var right = new StringWriter();
					WriteExpr(assign.Left, left, indent);
					WriteExpr(assign.Right, right, indent);
					writer.Write($"{left} = {right}");
					break;
				case ExpressionType.Block:
					var block = (BlockExpression)expr;
					writer.WriteLine($"{{");
					var nextIndent = indent + "..";
					foreach (var item in block.Expressions) {
						writer.Write(nextIndent);
						WriteExpr(item, writer, nextIndent);
						writer.WriteLine();
					}
					writer.Write($"{indent}}}");
					break;
				case ExpressionType.Call:
					var call = (MethodCallExpression)expr;
					var target = new StringWriter();
					var args = new StringWriter();
					if (call.Object is not null)
						WriteExpr(call.Object, target, indent);
					else target.Write(call.Method.DeclaringType.Name);
					var sep = string.Empty;
					foreach (var p in call.Arguments) {
						args.Write(sep);
						WriteExpr(p, args, indent);
						sep = ", ";
					}
					writer.Write($"{target}.{call.Method.Name}({args})");
					break;
				case ExpressionType.Coalesce:
					WriteCoalesce((BinaryExpression)expr, writer, indent);
					break;
				case ExpressionType.Conditional:
					WriteExpr((ConditionalExpression)expr, writer, indent);
					break;
				case ExpressionType.Constant:
					var cons = (ConstantExpression)expr;
					writer.Write(FormatValue(cons.Value));
					break;
				case ExpressionType.Convert:
					WriteConvert((UnaryExpression)expr, writer, indent);
					break;
				case ExpressionType.Lambda:
					var lambda = (LambdaExpression)expr;
					WriteExpr(lambda.Body, writer, indent);
					break;
				case ExpressionType.Invoke:
					WriteInvocation((InvocationExpression)expr, writer, indent);
					break;
				case ExpressionType.Parameter:
					var parameter = (ParameterExpression)expr;
					writer.Write($"{parameter.Name}");
					break;
				case ExpressionType.MemberAccess:
					WriteExpr((MemberExpression)expr, writer, indent);
					break;
				case ExpressionType.MemberInit:
					WriteMemberInit((MemberInitExpression)expr, writer, indent);
					break;
				case ExpressionType.New:
					WriteNew((NewExpression)expr, writer, indent);
					break;
				case ExpressionType.Equal:
					WriteOp((BinaryExpression)expr, "==", writer, indent);
					break;
				case ExpressionType.NotEqual:
					WriteOp((BinaryExpression)expr, "!=", writer, indent);
					break;
				default: writer.Write($"[{expr.NodeType}]"); break;
			}
		}

		static void WriteCoalesce(BinaryExpression expr, TextWriter writer, string indent) {
			WriteExpr(expr.Left, writer, indent);
			writer.Write(" ?? ");
			WriteExpr(expr.Right, writer, indent);
		}

		static void WriteConvert(UnaryExpression expr, TextWriter writer, string indent) {
			writer.Write($"({FormatType(expr.Type)})(");
			WriteExpr(expr.Operand, writer, indent);
			writer.Write(")");
		}

		static void WriteOp(BinaryExpression expr, string op, TextWriter writer, string indent) {
			WriteExpr(expr.Left, writer, indent);
			writer.Write($" {op} ");
			WriteExpr(expr.Right, writer, indent);
		}

		static void WriteExpr(ConditionalExpression expr, TextWriter writer, string indent) {
			if (expr.Type != typeof(void)) {
				WriteExpr(expr.Test, writer, indent);
				writer.Write(" ? ");
				WriteExpr(expr.IfTrue, writer, indent);
				writer.Write(" : ");
				WriteExpr(expr.IfFalse, writer, indent);
			}
			else {
				var test = new StringWriter();
				WriteExpr(expr.Test, test, indent);
				writer.WriteLine($"{indent}if({test}) {{");
				WriteExpr(expr.IfTrue, writer, indent + "  ");
				if (expr.IfFalse.NodeType != ExpressionType.Default) {
					writer.WriteLine("} else {");
					WriteExpr(expr.IfFalse, writer, indent + "  ");
				}
				writer.WriteLine($"{indent}}}");
			}
		}

		static void WriteExpr(MemberExpression expr, TextWriter writer, string indent) {
			var target = new StringWriter();
			WriteExpr(expr.Expression, target, indent);
			writer.Write($"{target}.{expr.Member.Name}");
		}

		static void WriteNew(NewExpression expr, TextWriter writer, string indent) {
			writer.Write($"new {FormatType(expr.Type)}(");
			var sep = string.Empty;
			foreach (var p in expr.Arguments) {
				writer.Write(sep);
				WriteExpr(p, writer, indent);
				sep = ", ";
			}
			writer.Write(')');
		}

		static void WriteMemberInit(MemberInitExpression expr, TextWriter writer, string indent) {
			WriteNew(expr.NewExpression, writer, indent);
			writer.Write(" {");
			var nextIndent = indent + "  ";
			foreach (var item in expr.Bindings) {
				writer.WriteLine();
				writer.Write($"{nextIndent}{item.Member.Name} = ");
				switch (item.BindingType) {
					default:
						writer.Write($"{item.BindingType}");
						break;

					case MemberBindingType.Assignment:
						var a = (MemberAssignment)item;
						WriteExpr(a.Expression, writer, nextIndent);
						break;
				}
			}
			writer.WriteLine();
			writer.Write($"{indent}}}");
		}

		static void WriteInvocation(InvocationExpression expr, TextWriter writer, string indent) {
			var args = new StringWriter();
			var sep = string.Empty;
			foreach (var p in expr.Arguments) {
				args.Write(sep);
				WriteExpr(p, args, indent);
				sep = ", ";
			}

			WriteExpr(expr.Expression, writer, indent);
			writer.Write(args);
		}

		static string FormatType(Type type) {
			if (type.IsGenericType) {
				var args = type.GetGenericArguments();
				var t = new StringWriter();
				t.Write('<');
				var sep = "";
				foreach (var item in args) {
					t.Write(sep);
					t.Write(FormatType(item));
					sep = ", ";
				}
				t.Write('>');
				return type.Name.Replace($"`{args.Length}", t.ToString());
			}
			return type.FullName switch {
				"System.Object" => "object",
				"System.Int32" => "int",
				_ => type.Name,
			};
		}

		static string FormatValue(object value) {
			if (value is null)
				return "null";
			var t = value.GetType();
			if (t.IsEnum)
				return $"{t.Name}.{value}";
			return t.FullName switch {
				"System.String" => $"\"{value}\"",
				"System.DBNull" => "DBNull.Value",
				_ => value.ToString(),
			};
		}
	}
}
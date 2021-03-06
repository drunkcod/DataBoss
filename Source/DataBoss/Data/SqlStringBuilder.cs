﻿using System.Text;

namespace DataBoss.Data
{
	public class SqlStringBuilder
	{
		readonly StringBuilder result = new StringBuilder();
		SqlQueryFormatting formatting;
		string beginBlock;
		string endBlock;
		string endElement;
		string indent;

		public char IndentChar = '\t';

		public SqlQueryFormatting Formatting 
		{
			get { return formatting; }
			set {
				formatting = value;
				indent = string.Empty;
				switch (value) {
					case SqlQueryFormatting.Default:
						beginBlock = " ";
						endElement = " ";
						endBlock = string.Empty;
						break;
					case SqlQueryFormatting.Indented:
						beginBlock = string.Empty;
						endElement = "\n";
						endBlock = "\n";
						break;
				}
			}
		}

		public SqlStringBuilder Space() => Append(" ");

		public SqlStringBuilder BeginBlock(string value) {
			if(result.Length > 0)
				Append(beginBlock);
			return Append(value);
		}

		public SqlStringBuilder EndBlock() =>
			Append(endBlock);

		public SqlStringBuilder Begin(string value) => Append(indent).Append(value);

		public SqlStringBuilder Append(string value) {
			result.Append(value);
			return this;
		}

		public SqlStringBuilder BeginIndent() {
			if (Formatting == SqlQueryFormatting.Default)
				return this;
			indent = new string(IndentChar, indent.Length + 1);
			return this;
		}

		public SqlStringBuilder EndIndent() {
			if(Formatting == SqlQueryFormatting.Default)
				return this;
			indent = new string(IndentChar, indent.Length - 1);
			return this;
		}

		public SqlStringBuilder EndElement() => Append(endElement);

		public override string ToString() => result.ToString();
	}
}

using System;
using System.Collections.Generic;

namespace DataBoss
{
	public class PowerArgsParseException : Exception
	{
		public class ParseFailure
		{
			public string ArgumentName;
			public Type ArgumentType;
			public string Input;
			public Exception Error;
		}

		public readonly IReadOnlyList<ParseFailure> Errors;

		internal PowerArgsParseException(IReadOnlyList<ParseFailure> errors) {
			this.Errors = errors;
		}
	}
}
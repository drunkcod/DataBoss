using System;
using System.ComponentModel.DataAnnotations;

namespace DataBoss
{
	public class PowerArgsValidationResult
	{
		public PowerArgsValidationResult(string name, Type type, object value, ValidationAttribute validation)
		{
			this.Name = name;
			this.ArgType = type;
			this.Value = value;
			this.Validation = validation;
		}

		public readonly string Name;
		public readonly Type ArgType;
		public string Message => Validation.FormatErrorMessage(Name);
		public readonly object Value;
		public ValidationAttribute Validation;
	}
}
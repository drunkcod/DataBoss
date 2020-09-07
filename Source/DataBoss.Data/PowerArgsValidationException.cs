using System;
using System.Collections.Generic;
using System.Linq;

namespace DataBoss
{
	public class PowerArgsValidationException : Exception
	{
		readonly IReadOnlyCollection<PowerArgsValidationResult> errors;

		public PowerArgsValidationException(ICollection<PowerArgsValidationResult> errors) {
			this.errors = errors.ToList().AsReadOnly();
		}

		public IReadOnlyCollection<PowerArgsValidationResult> Errors => errors;
	}
}
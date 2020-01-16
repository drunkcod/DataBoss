using System;

namespace DataBoss.Data
{
	public class InvalidConversionException : InvalidOperationException
	{
		public InvalidConversionException(string message, Type type) :base(message) { 
			this.Type = type;
		}

		public readonly Type Type;

		public override string Message => $"Error reading {Type}: " + base.Message;
	}
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace DataBoss.Specs
{
	class StubAttributeProvider : ICustomAttributeProvider
	{
		readonly List<object> attributes = new List<object>();

		public object[] GetCustomAttributes(bool inherit) {
			return attributes.ToArray();
		}

		public object[] GetCustomAttributes(Type attributeType, bool inherit) {
			return attributes.Where(attributeType.IsInstanceOfType).ToArray();
		}

		public bool IsDefined(Type attributeType, bool inherit) =>
			GetCustomAttributes(attributeType, inherit).Length > 0;

		public StubAttributeProvider Add(Attribute item) {
			attributes.Add(item);
			return this;
		}
	}
}
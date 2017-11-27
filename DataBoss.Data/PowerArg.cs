using System.ComponentModel.DataAnnotations;
using System.Reflection;

namespace DataBoss
{
	public class PowerArg
	{
		readonly MemberInfo memberInfo;

		public PowerArg(MemberInfo memberInfo) {
			this.memberInfo = memberInfo;
		}

		public string Name => memberInfo.Name;
		public bool IsRequired => memberInfo.GetCustomAttribute(typeof(RequiredAttribute)) != null;

		public override string ToString() {
			var r = $"-{Name} <{Name.ToLowerInvariant()}>";
			return IsRequired ? r : $"[{r}]";
		}
	}
}
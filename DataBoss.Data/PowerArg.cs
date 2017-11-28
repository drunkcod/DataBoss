using System;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;
using DataBoss.Linq;

namespace DataBoss
{
	public class PowerArg
	{
		readonly MemberInfo memberInfo;
		readonly PowerArgAttribute info;

		public PowerArg(MemberInfo memberInfo) {
			this.memberInfo = memberInfo;
			this.info = memberInfo.SingleOrDefault<PowerArgAttribute>();
		}

		static PowerArg FromMember(Type type, string memberName) =>
			 new PowerArg(type.GetMember(memberName).Single());

		public string Name => memberInfo.Name;
		public int? Order => info?.Order;
		public string Hint => info?.Hint
			?? memberInfo.SingleOrDefault<RegularExpressionAttribute>()?.Pattern
			?? Name.ToLowerInvariant();
		public string Description => memberInfo.SingleOrDefault<DescriptionAttribute>()?.Description 
			?? string.Empty;

		public bool IsRequired => memberInfo.Any<RequiredAttribute>();

		public override string ToString() {
			var r = $"-{Name} <{Hint}>";
			return IsRequired ? r : $"[{r}]";
		}
	}
}
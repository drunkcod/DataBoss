using System;
using System.Collections.Generic;
using System.Data;
using System.Text.RegularExpressions;

namespace DataBoss.DataPackage
{
	public class DataPackageResource
	{
		public readonly Func<IDataReader> GetData;
		public readonly string Name;
		public readonly List<string> PrimaryKey = new List<string>();
		public readonly List<DataPackageForeignKey> ForeignKeys = new List<DataPackageForeignKey>();

		public DataPackageResource(string name, Func<IDataReader> getData) {
			if(!Regex.IsMatch(name, @"^[a-z0-9-._]+$"))
				throw new NotSupportedException($"name MUST consist only of lowercase alphanumeric characters plus '.', '-' and '_' was '{name}'");
			this.Name = name;
			this.GetData = getData;
		}
	}
}

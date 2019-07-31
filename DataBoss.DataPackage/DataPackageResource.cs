using System;
using System.Data;

namespace DataBoss.DataPackage
{
	public class DataPackageResource
	{
		public readonly Func<IDataReader> GetData;
		public readonly string Name;

		public DataPackageResource(string name, Func<IDataReader> getData) {
			this.Name = name;
			this.GetData = getData;
		}
	}
}

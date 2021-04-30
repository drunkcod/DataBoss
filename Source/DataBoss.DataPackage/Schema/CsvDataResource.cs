using System;
using System.Data;
using DataBoss.DataPackage.Schema;

namespace DataBoss.DataPackage
{
	public class CsvDataResource : TabularDataResource
	{
		public string Delimiter;
		public bool HasHeaderRow;

		public CsvDataResource(DataPackageResourceDescription description, Func<IDataReader> getData) : base(description, getData, "csv") {
			this.HasHeaderRow = description.Dialect?.HasHeaderRow ?? true;
		}

		protected override TabularDataResource Rebind(string name, TabularDataSchema schema, Func<IDataReader> getData) =>
			new CsvDataResource(new DataPackageResourceDescription {
				Name = name,
				Schema = schema,
			}, getData) { Delimiter = Delimiter };

		protected override void UpdateDescription(DataPackageResourceDescription description) {
			description.Dialect = new CsvDialectDescription { 
				Delimiter = Delimiter,
				HasHeaderRow = HasHeaderRow,
			};
		}
	}
}

using System;
using System.Data;
using DataBoss.DataPackage.Schema;

namespace DataBoss.DataPackage
{
	public class CsvDataResource : TabularDataResource
	{
		public string Delimiter;
		public bool HasHeaderRow;

		public CsvDataResource(DataPackageResourceDescription description, ResourcePath path, Func<IDataReader> getData) : base(description, path, getData, "csv") {
			this.HasHeaderRow = description.Dialect?.HasHeaderRow ?? true;
		}

		protected override TabularDataResource Rebind(string name, TabularDataSchema schema, Func<IDataReader> getData) =>
			new CsvDataResource(new DataPackageResourceDescription {
				Name = name,
				Schema = schema,
			}, ResourcePath, getData) { Delimiter = Delimiter };

		protected override void UpdateDescription(DataPackageResourceDescription description) {
			description.Dialect = new CsvDialectDescription { 
				Delimiter = Delimiter,
				HasHeaderRow = HasHeaderRow,
			};
		}
	}
}

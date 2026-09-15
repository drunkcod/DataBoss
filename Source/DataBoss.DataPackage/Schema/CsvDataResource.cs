using System;
using System.Data;
using DataBoss.DataPackage.Schema;

namespace DataBoss.DataPackage
{
	public class CsvDataResource : TabularDataResource
	{
		public string Delimiter;
		public bool HasHeaderRow;

		public CsvDataResource(DataPackageResourceDescription description, ITabularDataSource source) : base(description, source, "csv") {
			this.HasHeaderRow = description.Dialect?.HasHeaderRow ?? true;
		}

		protected override TabularDataResource Rebind(string name, TabularDataSchema schema, ITabularDataSource source) =>
			new CsvDataResource(new DataPackageResourceDescription {
				Name = name,
				Schema = schema,
			}, source) {
				Delimiter = Delimiter,
				ResourcePath = ResourcePath,
			};

		protected override void UpdateDescription(DataPackageResourceDescription description) {
			if (description.Path.IsEmpty)
				description.Path = $"{Name}.csv";

			description.Dialect = new CsvDialectDescription {
				Delimiter = Delimiter,
				HasHeaderRow = HasHeaderRow,
			};
		}
	}
}

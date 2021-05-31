using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	public class InMemoryDataPackageStore
	{
		readonly Dictionary<string, MemoryStream> files = new();

		public void Save(DataPackage data, CultureInfo culture = null) {
			Clear();
			data.Save(OpenWrite, culture);
		}

		public IEnumerable<string> Files => files.Keys;

		public bool Contains(string path) => files.ContainsKey(path);

		public DataPackage Load() => DataPackage.Load(OpenRead);

		public byte[] ReadAllBytes(string path) => files[path].ToArray();

		public DataPackageDescription GetDataPackageDescription() {
			using var reader = new JsonTextReader(new StreamReader(OpenRead("datapackage.json"), Encoding.UTF8));
			return JsonSerializer.CreateDefault().Deserialize<DataPackageDescription>(reader);
		}

		void Clear() => 
			files.Clear();

		public Stream OpenWrite(string path) {
			var r = new MemoryStream();
			files.Add(path, r);
			return r;
		}

		public Stream OpenRead(string path) {
			var bytes = files[path];
			if (bytes.TryGetBuffer(out var buffer))
				return new MemoryStream(buffer.Array, buffer.Offset, buffer.Count, writable: false);
			return new MemoryStream(bytes.ToArray(), writable: false);
		}
	}
}

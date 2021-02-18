using System;
using System.Linq;
using CheckThat;
using Newtonsoft.Json;
using Xunit;

namespace DataBoss.DataPackage
{
	public class ResourcePathSpec
	{
		[Fact]
		public void empty() {
			var empty = new ResourcePath();

			Check.That(
				() => empty.IsEmpty,
				() => empty.Count == 0,
				() => empty.Count() == 0);
		}

		[Fact]
		public void single_item() {
			var path = "data.csv";
			ResourcePath resourcePath = path;

			Check.That(() => resourcePath == path);
		}

		[Fact]
		public void multi_item() {
			var paths = new[] {
				"part-1.csv",
				"part-2.csv",
			};
			ResourcePath resourcePath = paths;

			Check.That(
				() => resourcePath.Count == 2,
				() => resourcePath.SequenceEqual(paths));
		}

		[Fact]
		public void add_path_item() {
			var paths = new[] {
				"part-1.csv",
				"part-2.csv",
			};
			var resourcePath = new ResourcePath();

			resourcePath.Add(paths[0]);
			resourcePath.Add(paths[1]);

			Check.That(
				() => resourcePath.Count == 2,
				() => resourcePath.SequenceEqual(paths),
				() => paths.All(resourcePath.Contains));
		}

		[Fact]
		public void json_deserialization() {
			Check.That(
				() => FromJson("null").IsEmpty,
				() => FromJson("\"path.csv\"") == "path.csv",
				() => FromJson("[\"1.csv\", \"2.csv\"]").SequenceEqual(new[] { "1.csv", "2.csv" }));
		}

		static ResourcePath FromJson(string value) => JsonConvert.DeserializeObject<ResourcePath>(value);
	}
}

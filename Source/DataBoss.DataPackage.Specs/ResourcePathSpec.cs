using System;
using System.Linq;
using CheckThat;
using Newtonsoft.Json;
using Xunit;

namespace DataBoss.DataPackage
{
	public class ResourcePathSpec
	{
		/*
		 * ResourcePath exists to support the Frictionless Data Package spec where a resource path property 
		 * can be either a string or a array of strings. 
		 * In practice that means that we need to handle json objects of these two shapes:
		 * var simpleResource = { path: "data.csv" }
		 * var multipartResource = { path: ["part1.csv", "part2.csv"] }
		*/
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
		public void multipart() {
			var paths = new[] {
				"part-1.csv",
				"part-2.csv",
			};
			ResourcePath resourcePath = paths;

			Check.That(
				() => resourcePath.Count == 2,
				() => resourcePath.SequenceEqual(paths),
				() => resourcePath == string.Join(",", paths));
		}

		[Fact]
		public void add_path_item() {
			var paths = new[] {
				"part-1.csv",
				"part-2.csv",
			};
			var resourcePath = new ResourcePath {
				paths[0],
				paths[1]
			};

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

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using DataBoss.IO;
using Newtonsoft.Json;

namespace DataBoss.DataPackage
{
	[JsonConverter(typeof(ItemOrArrayJsonConverter))]
	public struct ResourcePath : IEquatable<ResourcePath>, ICollection<string>
	{
		interface IResourcePathState : IEnumerable<string>
		{
			int Count { get;  }

			IResourcePathState Add(string path);
		}

		class SinglePath : IResourcePathState
		{
			public readonly string Path;

			public SinglePath(string path) { this.Path = path; }

			public int Count => 1;

			public IResourcePathState Add(string path) => new MultiPath {
				Paths = new List<string> { this.Path, path }
			};

			public override string ToString() => Path;
			public override int GetHashCode() => Path.GetHashCode();

			public IEnumerator<string> GetEnumerator() {
				yield return Path;
			}

			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
		}

		class MultiPath : IResourcePathState
		{
			public List<string> Paths;

			public int Count => Paths.Count;

			public IResourcePathState Add(string path) {
				Paths.Add(path);
				return this;
			}

			public IEnumerator<string> GetEnumerator() => Paths.GetEnumerator();
			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

			public override string ToString() => string.Join(",", Paths);
		}

		IResourcePathState state;


		public bool IsEmpty => state == null || Count == 0;
		public int Count => state?.Count ?? 0;

		public bool IsReadOnly => false;

		ResourcePath(IResourcePathState state) {
			this.state = state;
		}

		public Stream OpenResourceStream(Func<string, Stream> open) {
			if (Count == 1)
				return open(this.First());
			return new ConcatStream(this.Select(open).GetEnumerator());
		}

		public override bool Equals(object obj) =>
			obj is ResourcePath other && Equals(other);

		public override string ToString() => state?.ToString() ?? string.Empty;

		public override int GetHashCode() => 
			state.GetHashCode();

		public bool Equals(ResourcePath other) => 
			ReferenceEquals(this, other) || other.ToString() == this.ToString();

		public void Add(string item) {
			state = state?.Add(item) ?? new SinglePath(item);
		}

		public void Clear() {
			throw new NotImplementedException();
		}

		public bool Contains(string item) {
			throw new NotImplementedException();
		}

		public void CopyTo(string[] array, int arrayIndex) {
			throw new NotImplementedException();
		}

		public bool Remove(string item) {
			throw new NotImplementedException();
		}

		public IEnumerator<string> GetEnumerator() => (state ?? Enumerable.Empty<string>()).GetEnumerator();
		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

		public static implicit operator ResourcePath(string path) => new ResourcePath(new SinglePath(path));
		public static implicit operator ResourcePath(string[] path) => new ResourcePath(new MultiPath { Paths = path.ToList() });
		public static implicit operator string(ResourcePath path) => path.ToString();

		public static bool operator ==(ResourcePath left, string path) => left.ToString() == path;
		public static bool operator !=(ResourcePath left, string path) => left.ToString() != path;
	}
}

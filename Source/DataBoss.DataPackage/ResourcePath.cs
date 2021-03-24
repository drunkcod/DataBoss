using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.IO.Compression;
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

			void Add(string path, out IResourcePathState next);
		}

		class EmptyPath : IResourcePathState
		{
			public int Count => 0;

			EmptyPath() { }

			public void Add(string path, out IResourcePathState next) =>
				next = new SinglePath(path);

			public IEnumerator<string> GetEnumerator() =>
				Enumerable.Empty<string>().GetEnumerator();

			IEnumerator IEnumerable.GetEnumerator() => 
				GetEnumerator();

			public static readonly EmptyPath Instance = new EmptyPath();

			public override string ToString() => string.Empty;
			public override int GetHashCode() => 0;
		}

		class SinglePath : IResourcePathState
		{
			public readonly string Path;

			public SinglePath(string path) { this.Path = path; }

			public int Count => 1;

			public void Add(string path, out IResourcePathState next) =>
				next = new MultiPath(ImmutableList<string>.Empty.AddRange(new [] { Path, path }));

			public override string ToString() => Path;
			public override int GetHashCode() => Path.GetHashCode();

			public IEnumerator<string> GetEnumerator() {
				yield return Path;
			}

			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
		}

		class MultiPath : IResourcePathState
		{
			readonly ImmutableList<string> paths;

			public int Count => paths.Count;

			public MultiPath(ImmutableList<string> paths) {
				this.paths = paths;
			}

			public void Add(string path, out IResourcePathState next) =>
				next = new MultiPath(paths.Add(path));

			public IEnumerator<string> GetEnumerator() => paths.GetEnumerator();
			IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

			public override string ToString() => string.Join(",", paths);
			public override int GetHashCode() =>
				Count == 0 ? 0 : paths[0].GetHashCode();
		}

		IResourcePathState state;
		IResourcePathState CurrentState => state ??= EmptyPath.Instance;

		public bool IsEmpty => CurrentState == EmptyPath.Instance;
		public int Count => CurrentState.Count;

		public bool IsReadOnly => false;

		ResourcePath(IResourcePathState state) {
			this.state = state;
		}

		public Stream OpenStream(Func<string, Stream> open) {
			if (Count == 1)
				return OpenResourceStream(this.First(), open);
			return new ConcatStream(this.Select(x => OpenResourceStream(x, open)).GetEnumerator());
		}

		static Stream OpenResourceStream(string path, Func<string, Stream> open) {
			var r = open(path);
			return (Path.GetExtension(path)) switch {
				".gz" => new GZipStream(r, CompressionMode.Decompress),
				_ => r,
			};
		}

		public override bool Equals(object obj) =>
			obj is ResourcePath other && Equals(other);

		public override string ToString() =>
			CurrentState.ToString();

		public override int GetHashCode() => 
			CurrentState.GetHashCode();

		public bool Equals(ResourcePath other) =>
			ReferenceEquals(this, other) || other.SequenceEqual(this);

		public void Add(string item) =>
			CurrentState.Add(item, out state);

		public void Clear() {
			state = EmptyPath.Instance;
		}

		public bool Contains(string item) {
			using var items = GetEnumerator();
			while (items.MoveNext())
				if (items.Current == item)
					return true;
			return false;
		}

		public void CopyTo(string[] array, int arrayIndex) {
			throw new NotImplementedException();
		}

		public bool Remove(string item) {
			throw new NotImplementedException();
		}

		public IEnumerator<string> GetEnumerator() => CurrentState.GetEnumerator();
		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

		public static implicit operator ResourcePath(string path) => new ResourcePath(new SinglePath(path));
		public static implicit operator ResourcePath(string[] path) => new ResourcePath(new MultiPath(ImmutableList<string>.Empty.AddRange(path)));
		public static implicit operator string(ResourcePath path) => path.ToString();

		public static bool operator ==(ResourcePath left, string path) => left.ToString() == path;
		public static bool operator !=(ResourcePath left, string path) => left.ToString() != path;
	}
}

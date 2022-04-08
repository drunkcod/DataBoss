using System;

namespace DataBoss.Data
{
	[DbType(typeof(string), required: true)]
	public struct RequiredString : IComparable<RequiredString>, IEquatable<RequiredString>
	{
		readonly string value;

		public RequiredString(string value) { this.value = value ?? throw new ArgumentNullException(nameof(value)); }

		public string Value => value;

		public static explicit operator RequiredString(string value) => new RequiredString(value);
		public static implicit operator string(RequiredString s) => s.value;

		public int CompareTo(RequiredString other) => this.value.CompareTo(other.Value);

		public bool Equals(RequiredString other) => this.value.Equals(other.Value);

		public override bool Equals(object obj) => obj is RequiredString other && Equals(other);

		public override int GetHashCode() => value.GetHashCode();

		public static bool operator ==(RequiredString left, RequiredString right) => left.Equals(right);

		public static bool operator !=(RequiredString left, RequiredString right) => !(left == right);

		public static bool operator <(RequiredString left, RequiredString right) => left.CompareTo(right) < 0;

		public static bool operator <=(RequiredString left, RequiredString right) => left.CompareTo(right) <= 0;

		public static bool operator >(RequiredString left, RequiredString right) => left.CompareTo(right) > 0;

		public static bool operator >=(RequiredString left, RequiredString right) => left.CompareTo(right) >= 0;
	}
}
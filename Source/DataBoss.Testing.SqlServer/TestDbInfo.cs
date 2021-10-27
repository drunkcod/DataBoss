using System;

namespace DataBoss.Testing.SqlServer
{
	public readonly struct TestDbInfo
	{
		public string Name { get; }
		public DateTime CreatedAt { get; }
		public string ConnectionString { get; }

		public TestDbInfo(string name, DateTime createdAt) : this(name, createdAt, string.Empty)
		{ }

		public TestDbInfo(string name, DateTime createdAt, string connectionString) {
			this.Name = name;
			this.CreatedAt = createdAt;
			this.ConnectionString = connectionString;
		}
	}
}

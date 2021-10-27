using System;

namespace DataBoss.Testing.SqlServer
{
	public readonly struct TestDbInfo
	{
		public string Name { get; }
		public DateTime CreatedAt { get; }

		public TestDbInfo(string name, DateTime createdAt) {
			this.Name = name;
			this.CreatedAt = createdAt;
		}
	}
}

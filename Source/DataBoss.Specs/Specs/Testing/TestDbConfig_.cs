using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CheckThat;
using DataBoss.Testing.SqlServer;
using Xunit;

namespace DataBoss.Testing
{
	public class TestDbConfig_Finalize
	{
		[Fact]
		public void null_gives_random_name() => Check
			.With(() => TestDbConfig.Finalize(null))
			.That(x => string.IsNullOrEmpty(x.Name) == false);

		[Fact]
		public void Name() => 
			Check.That(
				() => TestDbConfig.Finalize(new TestDbConfig { Name = "DatabaseName" }).Name == "DatabaseName",
				() => !string.IsNullOrEmpty(TestDbConfig.Finalize(new TestDbConfig { Name = null }).Name));

		[Fact]
		public void Server() => Check
			.With(() => new TestDbConfig { Server = "ServerName" })
			.That(x => TestDbConfig.Finalize(x).Server == x.Server);

		[Fact]
		public void Username() => Check
			.With(() => new TestDbConfig { Username = "User" })
			.That(x => TestDbConfig.Finalize(x).Username == x.Username);

		[Fact]
		public void Password() => Check
			.With(() => new TestDbConfig { Password= "Pass" })
			.That(x => TestDbConfig.Finalize(x).Password == x.Password);

		[Fact]
		public void ApplicationName() => Check
			.With(() => new TestDbConfig { ApplicationName = "AppName" })
			.That(x => TestDbConfig.Finalize(x).ApplicationName == x.ApplicationName);

	}
}

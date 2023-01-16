using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CheckThat;
using Xunit;
using DataBoss;

namespace DataBoss
{
	public class Uuid_
	{
		[Fact]
		public void create_md5_from_name() {
			//https://www.rfc-editor.org/rfc/rfc4122#appendix-B but the given value is wrong, see errata. The test is correct.
			Check.That(() => Uuid.NewMd5(Uuid.Dns, "www.widgets.com") == new Guid("3d813cbb-47fb-32ba-91df-831e1593ac29"));
		}
	}
}

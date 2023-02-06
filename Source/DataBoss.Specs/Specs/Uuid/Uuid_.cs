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

		[Fact]
		public void uuid_order_Parse() {
			var expected = Guid.Parse("70f9d048-ab83-495c-8593-e8797e503277");
			var uuidBytes = new byte[] { 0x70, 0xf9, 0xd0, 0x48, 0xab, 0x83, 0x49, 0x5c, 0x85, 0x93, 0xe8, 0x79, 0x7e, 0x50, 0x32, 0x77 };

			Check.That(
				() => Uuid.Parse(uuidBytes) == expected,
				//this row is here to show why we need the above.
				() => new Guid(uuidBytes) != expected);

		}
	}
}

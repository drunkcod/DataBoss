using System.Data;

namespace DataBoss.DataPackage
{
	public interface ITransformedDataRecord : IDataRecord
	{
		IDataRecord Source { get; }
	}
}

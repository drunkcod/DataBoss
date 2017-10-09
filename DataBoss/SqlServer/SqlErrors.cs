
namespace DataBoss.SqlServer
{
	public static class SqlErrorNumbers
	{
		public const int ConnectionTimeout = -2; //source: https://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqlerror.number(v=vs.110).aspx
		public const int TransactionWasDeadlocked = 1205;
	}
}

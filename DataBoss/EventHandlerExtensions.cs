using System;

namespace DataBoss
{
	static class EventHandlerExtensions
	{
		public static void Raise<T>(this EventHandler<T> handler,object sender, T args) where T : EventArgs
		{
			if(handler != null)
				handler(sender, args);
		}
	}
}
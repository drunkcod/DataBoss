using System;
using System.Collections.Generic;
using System.IO;

namespace DataBoss.Linq
{
	public static class TextReaderExtensions
	{
		public static IEnumerable<string> AsEnumerable(this Func<TextReader> self) {
			using(var text = self())
				for(string line; (line = text.ReadLine()) != null;)
					yield return line;
		}

		public static IEnumerable<T> Select<T>(this Func<TextReader> self, Func<string,T> selector) {
			using(var text = self())
				for(string line; (line = text.ReadLine()) != null;)
					yield return selector(line);
		} 
	}
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataBoss
{
	static class MissingLinqExtensions
	{
		public static TOutput[] ConvertAll<T,TOutput>(this T[] self, Converter<T,TOutput> converter) {
			return Array.ConvertAll(self, converter);
		}
	}
}

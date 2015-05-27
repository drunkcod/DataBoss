using System;
using System.Collections.Generic;

namespace DataBoss
{
    static class MissinqLinq
    {
        public static void ForEach<T>(this IEnumerable<T> self, Action<T> action) {
            foreach(var item in self)
                action(item);
        }
    }
}
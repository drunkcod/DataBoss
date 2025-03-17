using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace DataBoss.MongoDB;

public static class ChannelExtensions
{
    class SelectChannelReader<T, TOut> : ChannelReader<TOut>
    {
        readonly ChannelReader<T> inner;
        readonly Func<T, TOut> selector;

        public SelectChannelReader(ChannelReader<T> inner, Func<T, TOut> selector)
        {
            this.inner = inner;
            this.selector = selector;
        }

        public override bool TryRead([MaybeNullWhen(false)] out TOut item)
        {
            if (inner.TryRead(out var found))
            {
                item = selector(found);
                return true;
            }
            item = default;
            return false;
        }

        public override ValueTask<bool> WaitToReadAsync(CancellationToken cancellationToken = default) => inner.WaitToReadAsync(cancellationToken);
    }

    public static ChannelReader<TOut> Select<T, TOut>(this ChannelReader<T> reader, Func<T, TOut> transform) => new SelectChannelReader<T, TOut>(reader, transform);
}


using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace DataBoss.Data;

public static class Sync
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void GetResult(in ValueTask task)
    {
        if (task.IsCompleted) task.GetAwaiter().GetResult();
        task.AsTask().GetAwaiter().GetResult();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T GetResult<T>(in ValueTask<T> task) =>
        task.IsCompleted
        ? task.GetAwaiter().GetResult()
        : task.AsTask().GetAwaiter().GetResult();
}
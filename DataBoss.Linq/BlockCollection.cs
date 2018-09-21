using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace DataBoss.Collections
{
	public class BlockCollection<T> : ICollection<T>, IReadOnlyCollection<T>
	{
		class BlockNode
		{
			public T[] Items;
			public BlockNode Next;
		}

		readonly int blockSize;
		BlockNode head;
		BlockNode activeBlock;
		int count;
		int nextFreeSlot;

		public BlockCollection(int blockSize = 1024) {
			this.blockSize = blockSize;
			Clear();
		}

		public int Count => count;

		public bool IsReadOnly => false;

		public void Add(T item) {
			if (nextFreeSlot == blockSize)
				AddBlock();
			activeBlock.Items[nextFreeSlot] = item;
			++nextFreeSlot;
			++count;
		}

		public void Clear() {
			count = nextFreeSlot = 0;
			head = activeBlock = new BlockNode { Items = new T[blockSize] };
		}

		void AddBlock() {
			activeBlock.Next = new BlockNode { Items = new T[blockSize] };
			activeBlock = activeBlock.Next;
			nextFreeSlot = 0;
		}

		public bool Contains(T item) => this.AsEnumerable().Contains(item);

		public void CopyTo(T[] array, int arrayIndex) {
			foreach (var item in this)
				array[arrayIndex++] = item;
		}

		public IEnumerator<T> GetEnumerator() {
			for (var block = head; block != activeBlock; block = block.Next)
				foreach (var item in block.Items)
					yield return item;
			for (var i = 0; i != nextFreeSlot; ++i)
				yield return activeBlock.Items[i];
		}

		IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

		public bool Remove(T item) => throw new NotSupportedException();
	}
}
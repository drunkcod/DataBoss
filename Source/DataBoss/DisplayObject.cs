using System;

namespace DataBoss
{
	class DisplayObject
	{
		class NullObject
		{
			NullObject() { }

			public static readonly NullObject Instance = new NullObject();

			public override string ToString() => "null";
		}

		public readonly object VisualObject;
		public readonly Type ObjectType;

		public DisplayObject(Type objectType, object visualObject) {
			this.ObjectType= objectType;
			this.VisualObject = visualObject ?? NullObject.Instance;
		}
	}
}
using System;
using System.Collections.Generic;
using System.Text;
using System.Xml;

namespace DataBoss.Labs.DataBoss.Xml
{
	public static class XmlExtensions
	{
		public static void CopyNode(this XmlReader src, XmlWriter dst) {
			switch (src.NodeType) {
				default:
					throw new NotImplementedException($"{src.NodeType}");
				case XmlNodeType.XmlDeclaration:
					dst.WriteStartDocument();
					break;
				case XmlNodeType.Comment:
					dst.WriteComment(src.Value);
					break;
				case XmlNodeType.Element:
					dst.WriteStartElement(src.LocalName);
					if (src.HasAttributes)
						dst.WriteAttributes(src, true);
					if (src.IsEmptyElement)
						goto case XmlNodeType.EndElement;
					break;
				case XmlNodeType.EndElement:
					dst.WriteEndElement();
					break;
				case XmlNodeType.Whitespace:
					dst.WriteWhitespace(src.Value);
					break;
				case XmlNodeType.Text:
					dst.WriteString(src.Value);
					break;
			}
		}
	}
}

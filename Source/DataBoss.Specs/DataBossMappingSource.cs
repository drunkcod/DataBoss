using DataBoss.Data;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Linq.Mapping;
using System.Data.Linq.SqlClient;
using System.Linq;
using System.Reflection;

using LinqTableAttribute = System.Data.Linq.Mapping.TableAttribute;

namespace DataBoss.Specs
{
	class DataBossMappingSource : MappingSource
	{
		class DataBossTable : MetaTable
		{
			readonly MetaModel model;
			readonly Type rowType;

			public DataBossTable(MetaModel model, Type rowType) {
				this.model = model;
				this.rowType = rowType;
			}

			public override MetaModel Model => model;

			public override string TableName { 
				get { 
					var linqTable = rowType.GetCustomAttributes(typeof(LinqTableAttribute), true).Cast<LinqTableAttribute>().SingleOrDefault();
					if(linqTable != null)
						return linqTable.Name;
					var table = rowType.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.TableAttribute), true).Cast<System.ComponentModel.DataAnnotations.Schema.TableAttribute>().Single();
					return table.Name;
				} 
			}

			public override System.Reflection.MethodInfo DeleteMethod {
				get { throw new NotImplementedException(); }
			}

			public override System.Reflection.MethodInfo InsertMethod {
				get { throw new NotImplementedException(); }
			}

			public override MetaType RowType { get { return new DataBossRowType(this, rowType); }
			}

			public override System.Reflection.MethodInfo UpdateMethod {
				get { throw new NotImplementedException(); }
			}
		}

		class DataBossRowType : MetaType
		{
			static readonly ReadOnlyCollection<MetaAssociation> NoAssociations = new List<MetaAssociation>().AsReadOnly(); 
			static readonly ReadOnlyCollection<MetaDataMember> NoMembers = new List<MetaDataMember>().AsReadOnly(); 

			readonly MetaTable table;
			readonly Type type;
			readonly List<MetaDataMember> members = new List<MetaDataMember>(); 

			public DataBossRowType(MetaTable table, Type type) {
				this.table = table;
				this.type = type;

				foreach(var item in type.GetFields()) {
					var linqColumn = item.GetCustomAttributes(typeof(System.Data.Linq.Mapping.ColumnAttribute)).Cast<System.Data.Linq.Mapping.ColumnAttribute>().SingleOrDefault();
					if(linqColumn != null) {
						members.Add(new DataBossDataMember(this, item, item.FieldType, linqColumn.Name));
					}
					var column = item
						.GetCustomAttributes(typeof(System.ComponentModel.DataAnnotations.Schema.ColumnAttribute))
						.Cast<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>()
						.SingleOrDefault();
					if(column != null) {
						members.Add(new DataBossDataMember(this, item, item.FieldType, item.Name));
					}
				}
			}

			public override MetaTable Table => table;

			public override Type Type => type;

			public override ReadOnlyCollection<MetaAssociation> Associations => NoAssociations;

			public override ReadOnlyCollection<MetaDataMember> PersistentDataMembers => members.AsReadOnly();

			public override MetaType InheritanceRoot => null;

			public override MetaType GetInheritanceType(Type type) {
				if(type == this.type)
					return this;
				throw new NotSupportedException("Can't locate inheritance type for " + type.FullName);
			}

			public override bool IsEntity => false;

			public override ReadOnlyCollection<MetaDataMember> IdentityMembers => NoMembers;

			public override bool CanInstantiate {
				get { throw new NotImplementedException(); }
			}

			public override MetaDataMember DBGeneratedIdentityMember {
				get { throw new NotImplementedException(); }
			}

			public override System.Collections.ObjectModel.ReadOnlyCollection<MetaDataMember> DataMembers {
				get { throw new NotImplementedException(); }
			}

			public override System.Collections.ObjectModel.ReadOnlyCollection<MetaType> DerivedTypes {
				get { throw new NotImplementedException(); }
			}

			public override MetaDataMember Discriminator {
				get { throw new NotImplementedException(); }
			}

			public override MetaDataMember GetDataMember(MemberInfo member) {
				var foundMember = members.SingleOrDefault(x => x.Member == member);
				if(foundMember != null)
					return foundMember;
				throw new InvalidOperationException("No mapping known for member '" + member.Name + "' of " + Type.FullName);
			}

			public override MetaType GetTypeForInheritanceCode(object code) {
				throw new NotImplementedException();
			}

			public override bool HasAnyLoadMethod {
				get { throw new NotImplementedException(); }
			}

			public override bool HasAnyValidateMethod {
				get { throw new NotImplementedException(); }
			}

			public override bool HasInheritance => false;

			public override bool HasInheritanceCode {
				get { throw new NotImplementedException(); }
			}

			public override bool HasUpdateCheck {
				get { throw new NotImplementedException(); }
			}

			public override MetaType InheritanceBase {
				get { throw new NotImplementedException(); }
			}

			public override object InheritanceCode {
				get { throw new NotImplementedException(); }
			}

			public override MetaType InheritanceDefault {
				get { throw new NotImplementedException(); }
			}

			public override System.Collections.ObjectModel.ReadOnlyCollection<MetaType> InheritanceTypes {
				get { throw new NotImplementedException(); }
			}

			public override bool IsInheritanceDefault {
				get { throw new NotImplementedException(); }
			}

			public override MetaModel Model {
				get { throw new NotImplementedException(); }
			}

			public override string Name {
				get { throw new NotImplementedException(); }
			}

			public override System.Reflection.MethodInfo OnLoadedMethod {
				get { throw new NotImplementedException(); }
			}

			public override System.Reflection.MethodInfo OnValidateMethod {
				get { throw new NotImplementedException(); }
			}

			public override MetaDataMember VersionMember {
				get { throw new NotImplementedException(); }
			}
		}

		class DataBossDataMember : MetaDataMember
		{
			readonly MetaType declaringType;
			readonly MemberInfo member;
			readonly Type memberType;
			readonly string name;

			public DataBossDataMember(MetaType declaringType, MemberInfo member, Type memberType, string name) {
				this.declaringType = declaringType;
				this.member = member;
				this.memberType = memberType;
				this.name = name;
			}

			public override string MappedName => name;

			public override MemberInfo Member => member;

			public override bool IsAssociation => false;

			public override bool IsDeferred => false;

			public override Type Type => memberType;

			public override string DbType => DataBossDbType.From(memberType, member).ToString();

			public override MetaType DeclaringType => declaringType;

			public override MetaAssociation Association {
				get { throw new NotImplementedException(); }
			}

			public override AutoSync AutoSync {
				get { throw new NotImplementedException(); }
			}

			public override bool CanBeNull {
				get { throw new NotImplementedException(); }
			}

			public override MetaAccessor DeferredSourceAccessor {
				get { throw new NotImplementedException(); }
			}

			public override MetaAccessor DeferredValueAccessor {
				get { throw new NotImplementedException(); }
			}

			public override string Expression {
				get { throw new NotImplementedException(); }
			}

			public override bool IsDbGenerated {
				get { throw new NotImplementedException(); }
			}

			public override bool IsDeclaredBy(MetaType type) {
				throw new NotImplementedException();
			}

			public override bool IsDiscriminator {
				get { throw new NotImplementedException(); }
			}

			public override bool IsPersistent {
				get { throw new NotImplementedException(); }
			}

			public override bool IsPrimaryKey {
				get { throw new NotImplementedException(); }
			}

			public override bool IsVersion {
				get { throw new NotImplementedException(); }
			}

			public override System.Reflection.MethodInfo LoadMethod {
				get { throw new NotImplementedException(); }
			}

			public override MetaAccessor MemberAccessor {
				get { throw new NotImplementedException(); }
			}

			public override string Name {
				get { throw new NotImplementedException(); }
			}

			public override int Ordinal {
				get { throw new NotImplementedException(); }
			}

			public override MetaAccessor StorageAccessor {
				get { throw new NotImplementedException(); }
			}

			public override System.Reflection.MemberInfo StorageMember {
				get { throw new NotImplementedException(); }
			}

			public override UpdateCheck UpdateCheck {
				get { throw new NotImplementedException(); }
			}
		}

		class DataBossMetaModel : MetaModel
		{
			readonly Dictionary<Type, MetaTable> knownTables = new Dictionary<Type, MetaTable>(); 
			readonly Type contextType;
			
			public DataBossMetaModel(Type contextType) {
				this.contextType = contextType;
			}

			public override Type ContextType => contextType;

			public override Type ProviderType => typeof(Sql2005Provider);

			public override MetaTable GetTable(Type rowType) {
				if(knownTables.TryGetValue(rowType, out var cached))
					return cached;
				return knownTables[rowType] = new DataBossTable(this, rowType);
			}

			public override string DatabaseName {
				get { throw new NotImplementedException(); }
			}

			public override MetaFunction GetFunction(System.Reflection.MethodInfo method) {
				throw new NotImplementedException();
			}

			public override System.Collections.Generic.IEnumerable<MetaFunction> GetFunctions() {
				throw new NotImplementedException();
			}

			public override MetaType GetMetaType(Type type) {
				return GetTable(type).RowType;
			}

			public override System.Collections.Generic.IEnumerable<MetaTable> GetTables() {
				throw new NotImplementedException();
			}

			public override MappingSource MappingSource {
				get { throw new NotImplementedException(); }
			}
		}

		protected override MetaModel CreateModel(Type dataContextType) {
			return new DataBossMetaModel(dataContextType);
		}
	}
}
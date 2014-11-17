using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Telerik.OpenAccess;
using Telerik.OpenAccess.Metadata;

namespace TestClusterModel
{
    public partial class TestClusterModelContext : OpenAccessContext
    {
        static MetadataContainer metadataContainer = new TestClusterModelMetadataSource().GetModel();
        static BackendConfiguration backendConfiguration = new BackendConfiguration()
        {
            Backend = "mssql"
        };

        private const string DbConnection = @"data source=.\sqlexpress;initial catalog=FluentMappingDatabase;integrated security=True";

        public TestClusterModelContext()
            : base(DbConnection, backendConfiguration, metadataContainer)
        {

        }

        public IQueryable<Product> Products
        {
            get
            {
                return this.GetAll<Product>();
            }
        }

        public void UpdateSchema()
        {
            var handler = this.GetSchemaHandler();
            string script = null;
            try
            {
                script = handler.CreateUpdateDDLScript(null);
            }
            catch
            {
                bool throwException = false;
                try
                {
                    handler.CreateDatabase();
                    script = handler.CreateDDLScript();
                }
                catch
                {
                    throwException = true;
                }
                if (throwException)
                    throw;
            }

            if (string.IsNullOrEmpty(script) == false)
            {
                handler.ExecuteDDLScript(script);
            }
        }
    }
}

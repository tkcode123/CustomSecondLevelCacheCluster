using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Telerik.OpenAccess;
using Telerik.OpenAccess.Metadata;

namespace $rootnamespace$
{
    public partial class $rootnamespace$Context : OpenAccessContext
    {
        static MetadataContainer metadataContainer = new $rootnamespace$MetadataSource().GetModel();
        static BackendConfiguration backendConfiguration = new BackendConfiguration()
        {
            Backend = "mssql"
        };

        private const string DbConnection = @"data source=.\sqlexpress;initial catalog=FluentMappingDatabase;integrated security=True";

        public $rootnamespace$Context()
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

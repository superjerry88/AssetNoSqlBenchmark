using System;

using Raven.Client.Documents;

namespace AssetManagementNoSql
{
    public class DocumentStoreHolder
    {
        private static readonly Lazy<IDocumentStore> _store = new Lazy<IDocumentStore>(CreateDocumentStore);

        private static IDocumentStore CreateDocumentStore()
        {
            string serverURL = "http://127.0.0.1:57777";
            string databaseName = "TestDb1";

            IDocumentStore documentStore = new DocumentStore
            {
                Urls = new[] { serverURL },
                Database = databaseName
            };

            documentStore.Initialize();
            return documentStore;
        }

        public static IDocumentStore Store => _store.Value;
    }
}

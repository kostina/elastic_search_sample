using Nest;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ElasticSearch.Sample
{
      public class ElasticSearchProvider<T> where T : class
    {
        ElasticClient _client = null;

        private string _defaultIndex;

        public ElasticSearchProvider(string connectionString, string defaultIndex)
        {
            if (string.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            if (string.IsNullOrWhiteSpace(defaultIndex))
            {
                throw new ArgumentNullException(nameof(defaultIndex));
            }

            var uri = new Uri(connectionString);
            var settings = new ConnectionSettings(uri);
            _client = new ElasticClient(settings);

            _defaultIndex = defaultIndex;

            settings.DefaultIndex(_defaultIndex);
        }

        public List<T> List()
        {
            if (_client.IndexExists(_defaultIndex).Exists)
            {
                var response = _client.Search<T>();
                return response.Documents.ToList();
            }

            return null;
        }

        public async System.Threading.Tasks.Task AddNewIndexAsync(T model)
        {
            await _client.IndexAsync(model, null);
        }

        public List<T> List(string condition, int from, int count)
        {
            if (_client.IndexExists(_defaultIndex).Exists)
            {
                var query = condition;

                return _client.SearchAsync<T>(s => s
                    .From(from)
                    .Take(count)
                    .Query(qry => qry
                        .Bool(b => b
                            .Must(m => m
                                .QueryString(qs => qs
                                    .DefaultField("_all")
                                    .Query(query)))))).Result.Documents.ToList();
            }

            return null;
        }

    }
}

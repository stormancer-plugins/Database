using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nest;
using Stormancer;
using Stormancer.Plugins;
using Stormancer.Server.Components;
using System.Collections.Concurrent;
using Server.Plugins.Configuration;
using Stormancer.Diagnostics;
using Newtonsoft.Json.Linq;
using Elasticsearch.Net;
using Nest.JsonNetSerializer;

namespace Server.Database
{
    public class Startup
    {
        public void Run(IAppBuilder builder)
        {
            builder.AddPlugin(new ESClientPlugin());

        }
    }


    public class ESConnectionPoolConfig
    {
        public List<string> Endpoints { get; set; } = new List<string> { "http://localhost:9200" };
        public bool Sniffing { get; set; } = true;
    }
    public class ESIndexPolicyConfig
    {

        public int RetryTimeout { get; set; } = 5;
        public int MaxRetries { get; set; } = 5;
        public string Pattern { get; set; }
        public string ConnectionPool { get; set; } = "default";

    }

    public class ESConfig
    {
        public Dictionary<string, JObject> Indices { get; set; } = new Dictionary<string, JObject>();
        public Dictionary<string, ESConnectionPoolConfig> ConnectionPools { get; set; } = new Dictionary<string, ESConnectionPoolConfig>();
    }

    internal class ESClientPlugin : IHostPlugin
    {
        private object synclock = new object();

        public void Build(HostPluginBuildContext ctx)
        {

            ctx.HostDependenciesRegistration += (IDependencyBuilder b) =>
            {
                b.Register<ESClientFactory>().As<IESClientFactory>().SingleInstance();
                b.Register<ESClientFactoryEventHandler>().As<IESClientFactoryEventHandler>();
                SmartFormat.Smart.Default.AddExtensions(new TimeIntervalFormatter());


            };

        }
    }
    public class ConnectionParameters
    {
        public string ConnectionPool { get; set; }
        public string IndexName { get; set; }
        public int maxRetries { get; set; }
        public int retryTimeout { get; set; }
    }

    /// <summary>
    ///Provide access to Elasticsearch DBs using centralized configuration policies
    /// </summary>
    /// <example>
    /// Configuration
    /// -------------
    /// 
    /// {
    ///     "elasticsearch":{
    ///         "indices":{
    ///             "gameData":{
    ///                 "retryTimeout":5,
    ///                 "maxRetries":5,
    ///                 "connectionPool":"game",
    ///                 
    ///             },
    ///             "leaderboards":{
    ///                 "retryTimeout":5,
    ///                 "maxRetries":5,
    ///                 "connectionPool":"game",
    ///                 "pattern":"leaderboards-{args.0}"
    ///             },
    ///             "analytics":{
    ///                 "retryTimeout":5,
    ///                 "maxRetries":1,
    ///                 "connectionPool":"analytics",
    ///                 //Pattern used to compute the actual index name. interval makes date interval based index names: 
    ///                 //interval(24) is daily, interval(168) weekly, interval(1) hourly, et...
    ///                 "pattern":"analytics-{type}-{args.0:interval(168)}"
    ///             }
    ///         },
    ///         "connectionPools":{
    ///             "game":{
    ///                 "sniffing":true, //Is the connection able to obtain updated endpoint info from the cluster (default to true)
    ///                 "endpoints":["http://localhost:9200"] //elasticsearch endpoints
    ///             },
    ///             "analytics":{
    ///                 "sniffing":true,
    ///                 "endpoints":["http://localhost:9200"]
    ///             }
    ///         }
    ///     }
    /// }
    /// 
    /// </example>
    public interface IESClientFactory
    {
        Task<Nest.IElasticClient> EnsureMappingCreated<T>(string name, Func<PutMappingDescriptor<T>, IPutMappingRequest> mappingDefinition, params object[] parameters) where T : class;
        Task<Nest.IElasticClient> CreateClient<T>(string name, params object[] parameters);
        Task<Nest.IElasticClient> CreateClient(string type, string name, params object[] parameters);
        ConnectionParameters GetConnectionParameters<T>(string name, params object[] parameters);
        string GetIndex<T>(string name, params object[] parameters);
        string GetIndex(string type, string name, params object[] parameters);
        Elasticsearch.Net.IConnectionPool GetConnectionPool(string id);
        Nest.IElasticClient CreateClient(ConnectionParameters p);

    }
    public class IndexNameFormatContext
    {
        public string type;
        public string name;
        public object[] args;

        public Dictionary<string, object> ctx = new Dictionary<string, object>();
    }

    public interface IESClientFactoryEventHandler
    {
        void OnCreatingIndexName(IndexNameFormatContext ctx);
    }


    class ESClientFactory : IESClientFactory, IDisposable
    {
        private const string LOG_CATEGORY = "ESClientFactory";

        private static ConcurrentDictionary<string, Task> _mappingInitialized = new ConcurrentDictionary<string, Task>();
        private IEnvironment _environment;
        private ConcurrentDictionary<string, Nest.ElasticClient> _clients = new ConcurrentDictionary<string, ElasticClient>();
        private Dictionary<string, Elasticsearch.Net.IConnectionPool> _connectionPools;

        private ESConfig _config;
        private readonly ILogger _logger;
        private readonly Func<IEnumerable<IESClientFactoryEventHandler>> _eventHandlers;

        //private List<Elasticsearch.Net.Connection.HttpClientConnection> _connections = new List<Elasticsearch.Net.Connection.HttpClientConnection>();
        public ESClientFactory(IEnvironment environment, IConfiguration configuration, ILogger logger, Func<IEnumerable<IESClientFactoryEventHandler>> eventHandlers)
        {
            _eventHandlers = eventHandlers;
            _environment = environment;

            _logger = logger;
            configuration.SettingsChanged += (_, settings) => ApplySettings(settings);
            ApplySettings(configuration.Settings);
        }

        private void ApplySettings(dynamic config)
        {
            _clients.Clear();
            _config = (ESConfig)(config?.elasticsearch?.ToObject<ESConfig>()) ?? new ESConfig();

            _connectionPools = _config.ConnectionPools?.ToDictionary(kvp => kvp.Key, kvp =>
            {
                var c = kvp.Value;
                var endpoints = c.Endpoints.DefaultIfEmpty("http://localhost:9200").Select(endpoint => new Uri(endpoint));
                if (c.Sniffing)
                {
                    //      var connectionEndpoints = ((JArray)config.esEndpoints).ToObject<string[]>();
                    return (Elasticsearch.Net.IConnectionPool)(new Elasticsearch.Net.SniffingConnectionPool(endpoints));
                }
                else
                {
                    return (Elasticsearch.Net.IConnectionPool)(new Elasticsearch.Net.StaticConnectionPool(endpoints));
                }
            }) ?? new Dictionary<string, Elasticsearch.Net.IConnectionPool>();

            if (!_connectionPools.ContainsKey("default"))
            {
                _connectionPools.Add("default", new Elasticsearch.Net.SniffingConnectionPool(new[] { new Uri("http://localhost:9200") }));
            }
        }

        public Task<IElasticClient> CreateClient<T>(string name, object[] parameters)
        {
            return CreateClient(typeof(T).Name, name, parameters);

        }

        public Task<Nest.IElasticClient> CreateClient(string type, string name, params object[] parameters)
        {

            var p = GetConnectionParameters(type, name, parameters);
            return Task.FromResult(CreateClient(p));
        }

        public void Dispose()
        {
            foreach (var pool in _connectionPools.Values)
            {
                pool.Dispose();
            }
        }
       
        public ConnectionParameters GetConnectionParameters<T>(string name, params object[] parameters)
        {
            return GetConnectionParameters(typeof(T).Name.ToLowerInvariant(), name, parameters);
        }

        public ConnectionParameters GetConnectionParameters(string type, string name, params object[] parameters)
        {
            string indexName = null;
            JObject indexConfig;
            ESIndexPolicyConfig policyConfig;
            if (_config.Indices.TryGetValue(name, out indexConfig))
            {
                policyConfig = indexConfig.ToObject<ESIndexPolicyConfig>();
            }
            else
            {
                policyConfig = new ESIndexPolicyConfig();
            }

            var formatCtx = new IndexNameFormatContext { args = parameters, type = type, name = name };

            _eventHandlers().RunEventHandler<IESClientFactoryEventHandler>( e => e.OnCreatingIndexName(formatCtx), ex => {
                _logger.Log(Stormancer.Diagnostics.LogLevel.Error, LOG_CATEGORY, "An error occured while running an 'database.OnCreate' event handler", ex);
            });

            if (policyConfig.Pattern != null)
            {
                indexName = SmartFormat.Smart.Format(policyConfig.Pattern, formatCtx);
            }

            if (string.IsNullOrWhiteSpace(indexName))
            {
                indexName = $"{name}-{type}";
            }

            return new ConnectionParameters { ConnectionPool = policyConfig.ConnectionPool, IndexName = indexName.ToLowerInvariant(), maxRetries = policyConfig.MaxRetries, retryTimeout = policyConfig.RetryTimeout };
        }



        public IElasticClient CreateClient(ConnectionParameters p)
        {           
            Elasticsearch.Net.IConnectionPool connectionPool;
            if (!_connectionPools.TryGetValue(p.ConnectionPool, out connectionPool))
            {
                _logger.Log(Stormancer.Diagnostics.LogLevel.Trace, "es", "Failed to find connection Pool", new { });
                throw new InvalidOperationException($"Failed to find connection pool {p.ConnectionPool} in elasticsearch config.");
            }

            return _clients.GetOrAdd(p.IndexName, i =>
            {
                return new Nest.ElasticClient(new ConnectionSettings(connectionPool, JsonNetSerializer.Default).DefaultIndex(p.IndexName.ToLowerInvariant()).MaximumRetries(p.maxRetries).MaxRetryTimeout(TimeSpan.FromSeconds(p.retryTimeout)));
            });
        }

        public string GetIndex<T>(string name, params object[] parameters)
        {
            return GetIndex(typeof(T).Name, name, parameters);
        }

        public string GetIndex(string type, string name, params object[] parameters)
        {
            return GetConnectionParameters(type, name, parameters).IndexName;
        }

        public IConnectionPool GetConnectionPool(string id)
        {
            IConnectionPool connection;
            if (_connectionPools.TryGetValue(id, out connection))
            {
                return connection;
            }
            else
            {
                _connectionPools.TryGetValue("default", out connection);
                return connection;
            }
        }

        public async Task<IElasticClient> EnsureMappingCreated<T>(string name, Func<PutMappingDescriptor<T>, IPutMappingRequest> mappingDefinition, params object[] parameters) where T : class
        {
            var client = await CreateClient<T>(name, parameters);

            await _mappingInitialized.GetOrAdd(client.ConnectionSettings.DefaultIndex, index => CreateMapping<T>(client, mappingDefinition));

            return client;
        }

        private async  Task CreateMapping<T>(IElasticClient client, Func<PutMappingDescriptor<T>, IPutMappingRequest> mapping) where T : class
        {
            if (!(await client.IndexExistsAsync(client.ConnectionSettings.DefaultIndex)).Exists)
            {
                await client.CreateIndexAsync(client.ConnectionSettings.DefaultIndex);
                await client.MapAsync<T>(mapping);
            }
        }
    }
}

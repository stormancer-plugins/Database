using Server.Plugins.Configuration;

namespace Server.Database
{
    public  class ESClientFactoryEventHandler : IESClientFactoryEventHandler
    {
        private readonly IConfiguration _config;
        public ESClientFactoryEventHandler(IConfiguration config)
        {
            _config = config;
        }

        public void OnCreatingIndexName(IndexNameFormatContext ctx)
        {
            ctx.ctx.Add("season", _config.Settings.season);
        }
    }
}

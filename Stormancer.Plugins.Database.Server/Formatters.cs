using SmartFormat.Core.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Server.Database
{
    internal class TimeIntervalFormatter : IFormatter
    {
       
        private string[] _names = new string[] { "interval" };
        public string[] Names
        {
            get
            {
                return _names;
            }

            set
            {
                _names = value;
            }
        }

        public bool TryEvaluateFormat(IFormattingInfo formattingInfo)
        {
            if (!(formattingInfo.CurrentValue is DateTime))
            {
                return false;
            }
            DateTime date = (DateTime)formattingInfo.CurrentValue;

            int interval;
            if (!int.TryParse(formattingInfo.FormatterOptions, out interval))
            {
                return false;
            }

            var ts = date.Ticks / (TimeSpan.TicksPerHour * interval);
            formattingInfo.Write(ts.ToString());
            return true;

        }
    }
}

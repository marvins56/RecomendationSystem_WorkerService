using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RecomendationSystemWorkerService.Models.Helpers
{
    public class Helpers
    {
        private readonly ILogger<Helpers> _logger;
    
        public Helpers(
            ILogger<Helpers> logger
         )
        {
            _logger = logger;
          
        }

        public Helpers()
        {
        }

        public bool TryParseInt(string value, out int result)
        {
            // First, attempt to parse the value directly as an integer.
            if (int.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result))
            {
                return true;
            }

            // If direct parsing fails, try to parse as a float and then convert to int.
            if (float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out float floatValue))
            {
                // Here, you can choose to round or truncate.
                result = (int)Math.Round(floatValue); // Use Math.Round to round to the nearest integer.
                return true;
            }

            // If both attempts fail, set the result to a default value and return false.
            result = 0; // Default value if parsing is unsuccessful.
            return false;
        }
        public bool TryParseLong(string value, out long result)
        {
            return long.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result);
        }
        public bool TryParseFloat(string value, out float result)
        {
            return float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out result);
        }
        public bool TryParseBool(string value, out bool result)
        {
            // Define what you consider to be true or false in your data
            var trueValues = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "true", "yes", "1", "t" };
            var falseValues = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "false", "no", "0", "f" };

            if (trueValues.Contains(value))
            {
                result = true;
                return true;
            }
            else if (falseValues.Contains(value))
            {
                result = false;
                return true;
            }
            else
            {
                _logger.LogError($"Invalid boolean value: '{value}'");
                result = false; // Decide on a default value for your scenario
                return false;
            }
        }
        public bool TryParseDateTime(string value, out DateTime result)
        {
            // Handle ISO 8601 date format (yyyy-MM-dd) and year-only format
            string[] formats = { "yyyy-MM-dd", "yyyy" };
            return DateTime.TryParseExact(value, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out result);
        }
        public bool TryParseIntFromMixedFormat(string value, out int result)
        {
            // First, attempt to parse the value directly as an integer.
            if (int.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result))
            {
                return true;
            }

            // If direct parsing fails, try to parse as a float and then convert to int.
            if (float.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out float floatValue))
            {
                // Here, you can choose to round or truncate.
                result = (int)Math.Round(floatValue); // Use Math.Round to round to the nearest integer.
                return true;
            }

            // If both attempts fail, set the result to a default value and return false.
            result = 0; // Default value if parsing is unsuccessful.
            return false;
        }
    }
}

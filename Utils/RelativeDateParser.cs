using System;
using System.Text.RegularExpressions;

namespace Ceasier.Utils
{
    public static class RelativeDateParser
    {
        private const string ValidUnits = "year|month|week|day|hour|minute|second|invalid";

        /// <summary>
        /// Ex: "last year"
        /// </summary>
        private static readonly Regex _basicRelativeRegex = new Regex(@"^(last|next) +(" + ValidUnits + ")$");

        /// <summary>
        /// Ex: "+1 week"
        /// Ex: " 1week"
        /// </summary>
        private static readonly Regex _simpleRelativeRegex = new Regex(@"^([+-]?\d+) *(" + ValidUnits + ")s?$");

        /// <summary>
        /// Ex: "2 minutes"
        /// Ex: "3 months 5 days 1 hour ago"
        /// </summary>
        private static readonly Regex _completeRelativeRegex = new Regex(@"^(?: *(\d+) *(" + ValidUnits + ")s?)+( +ago)?$");

        public static DateTime Parse(string input)
        {
            // Remove the case and trim spaces.
            var txt = input.Trim().ToLower();

            return
                // Try common simple words like "yesterday".
                TryParseCommonDateTime(txt) ??
                // Try common simple words like "last week".
                TryParseLastOrNextCommonDateTime(txt) ??
                // Try simple format like "+1 week".
                TryParseSimpleRelativeDateTime(txt) ??
                // Try first the full format like "1 day 2 hours 10 minutes ago".
                TryParseCompleteRelativeDateTime(txt) ??
                // Try parse fixed dates like "01/01/2000".
                DateTime.Parse(txt);
        }

        private static DateTime? TryParseCommonDateTime(string input)
        {
            switch (input)
            {
                case "now":
                    return DateTime.Now;
                case "today":
                    return DateTime.Today;
                case "tomorrow":
                    return DateTime.Today.AddDays(1);
                case "yesterday":
                    return DateTime.Today.AddDays(-1);
                default:
                    return null;
            }
        }

        private static DateTime? TryParseLastOrNextCommonDateTime(string input)
        {
            var match = _basicRelativeRegex.Match(input);

            if (!match.Success)
            {
                return null;
            }

            var unit = match.Groups[2].Value;
            var sign = string.Compare(match.Groups[1].Value, "next", true) == 0 ? 1 : -1;

            return AddOffset(unit, sign);
        }

        private static DateTime? TryParseSimpleRelativeDateTime(string input)
        {
            var match = _simpleRelativeRegex.Match(input);

            if (!match.Success)
            {
                return null;
            }

            var delta = Convert.ToInt32(match.Groups[1].Value);
            var unit = match.Groups[2].Value;

            return AddOffset(unit, delta);
        }

        private static DateTime? TryParseCompleteRelativeDateTime(string input)
        {
            var match = _completeRelativeRegex.Match(input);

            if (!match.Success)
            {
                return null;
            }

            var values = match.Groups[1].Captures;
            var units = match.Groups[2].Captures;
            var sign = match.Groups[3].Success ? -1 : 1;
            var dateTime = UnitIncludeTime(units) ? DateTime.Now : DateTime.Today;

            for (int i = 0; i < values.Count; ++i)
            {
                var value = sign * Convert.ToInt32(values[i].Value);
                var unit = units[i].Value;

                dateTime = AddOffset(unit, value, dateTime);
            }

            return dateTime;
        }

        /// <summary>
        /// Add/Remove years/days/hours... to a datetime.
        /// </summary>
        /// <param name="unit">Must be one of ValidUnits</param>
        /// <param name="value">Value in given unit to add to the datetime</param>
        /// <param name="dateTime">Relative datetime</param>
        /// <returns>Relative datetime</returns>
        private static DateTime AddOffset(string unit, int value, DateTime dateTime)
        {
            switch (unit)
            {
                case "year":
                    return dateTime.AddYears(value);
                case "month":
                    return dateTime.AddMonths(value);
                case "week":
                    return dateTime.AddDays(value * 7);
                case "day":
                    return dateTime.AddDays(value);
                case "hour":
                    return dateTime.AddHours(value);
                case "minute":
                    return dateTime.AddMinutes(value);
                case "second":
                    return dateTime.AddSeconds(value);
                default:
                    throw new Exception("Internal error: Unhandled relative date/time case.");
            }
        }

        /// <summary>
        /// Add/Remove years/days/hours... relative to today or now.
        /// </summary>
        /// <param name="unit">Must be one of ValidUnits</param>
        /// <param name="value">Value in given unit to add to the datetime</param>
        /// <returns>Relative datetime</returns>
        private static DateTime AddOffset(string unit, int value) => AddOffset(unit, value, UnitIncludesTime(unit) ? DateTime.Now : DateTime.Today);

        private static bool UnitIncludeTime(CaptureCollection units)
        {
            foreach (Capture unit in units)
            {
                if (UnitIncludesTime(unit.Value))
                {
                    return true;
                }
            }

            return false;
        }

        private static bool UnitIncludesTime(string unit) => (
            "hour" == unit || "minute" == unit || "second" == unit
        );
    }
}

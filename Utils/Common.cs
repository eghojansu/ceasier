using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Ceasier.Utils
{
    public static class Common
    {
        public static string Seed(int length) => Path.GetRandomFileName().Replace(".", "").Substring(0, length);

        public static string Seed() => Seed(8);

        public static string CaseTitle(string text)
        {
            var title = "";
            var words = text.Split(new char[] { '-', '_' });

            foreach (var word in words)
            {
                if (word.Length == 0)
                {
                    continue;
                }

                if (word.Length < 3 || word.Any(char.IsDigit))
                {
                    title += $" {word.ToUpper()}";
                }
                else
                {
                    title += $" {word.Substring(0, 1).ToUpper()}{word.Substring(1).ToLower()}";
                }
            }

            return title;
        }

        public static List<KeyValuePair<string, object>> ObjectValues(object sets)
        {
            if (sets == null)
            {
                return new List<KeyValuePair<string, object>>();
            }

            if (sets is List<KeyValuePair<string, object>> dList)
            {
                return dList;
            }

            if (sets is Dictionary<string, object> dDict)
            {
                return dDict.ToList();
            }

            var list = new List<KeyValuePair<string, object>>();

            foreach (var prop in sets.GetType().GetProperties())
            {
                list.Add(new KeyValuePair<string, object>(prop.Name, prop.GetValue(sets, null)));
            }

            return list;
        }
    }
}

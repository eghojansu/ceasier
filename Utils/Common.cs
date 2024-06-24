using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Ceasier.Utils
{
    public static class Common
    {
        public static readonly Type FloatType = typeof(float);
        public static readonly Type DoubleType = typeof(double);
        public static readonly Type IntType = typeof(int);
        public static readonly Type DecimalType = typeof(decimal);
        public static readonly Type DateTimeType = typeof(DateTime);
        public static readonly Type StringType = typeof(string);

        public static string Seed(int length) => Path.GetRandomFileName().Replace(".", "").Substring(0, length);

        public static string Seed() => Seed(8);

        public static string CharSequence(int n)
        {
            string result = "";

            while (n > 0)
            {
                n--;
                result = (char)('A' + n % 26) + result;
                n /= 26;
            }

            return result;
        }

        public static string CaseTitle(string text)
        {
            var title = "";
            var glue = false;
            var words = text.Split(new char[] { ' ', '-', '_' });

            foreach (var word in words)
            {
                if (word.Length == 0)
                {
                    continue;
                }

                if (glue)
                {
                    title += " ";
                }

                if (word.Length < 3 || word.Any(char.IsDigit))
                {
                    title += word.ToUpper();
                }
                else
                {
                    title += $"{word.Substring(0, 1).ToUpper()}{word.Substring(1).ToLower()}";
                }

                glue = true;
            }

            return title;
        }

        public static void ObjectMap(object sets, Action<string, object> action)
        {
            if (null == sets)
            {
                return;
            }

            if (sets is List<KeyValuePair<string, object>> dList)
            {
                foreach (var item in dList)
                {
                    action(item.Key, item.Value);
                }

                return;
            }
            
            if (sets is Dictionary<string, object> dDict)
            {
                foreach (var item in dDict)
                {
                    action(item.Key, item.Value);
                }

                return;
            }

            foreach (var prop in sets.GetType().GetProperties())
            {
                action(prop.Name, prop.GetValue(sets, null));
            }
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

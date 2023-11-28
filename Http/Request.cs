using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text.RegularExpressions;
using System.Web;

namespace Ceasier.Http
{
    public class Request
    {
        public readonly NameValueCollection Queries;
        public readonly NameValueCollection Form;

        public Request(NameValueCollection queries, NameValueCollection form)
        {
            Queries = queries ?? new NameValueCollection();
            Form = form;
        }

        public Request(string queryString) : this(HttpUtility.ParseQueryString(queryString), null)
        {
        }

        public Request() : this(HttpContext.Current?.Request.QueryString, HttpContext.Current?.Request.Form)
        {
        }

        public T Value<T>(string name)
        {
            object value = Form?[name] ?? GetQuery(name);

            return null == value ? default : (T)Convert.ChangeType(value, typeof(T));
        }

        public string Value(string name) => Value<string>(name)?.Replace("*", "%");

        public Dictionary<string, List<object>> GetQueries(params string[] names)
        {
            var result = new Dictionary<string, List<object>>();

            foreach (var name in names)
            {
                var pos = name.IndexOf('.');
                var pattern = @"^";

                if (pos < 0)
                {
                    pattern += name;
                } else
                {
                    pattern += name.Substring(0, pos) + @"\[" + string.Join(@"\]\[", name.Substring(pos + 1).Split('.')) + @"\]";
                }

                pattern = pattern.Replace("*", @".*") + @"$";

                var regex = new Regex(pattern, RegexOptions.IgnoreCase);
                var value = new List<object>();

                foreach (string key in Queries.AllKeys)
                {
                    if (regex.IsMatch(key))
                    {
                        value.Add(Queries[key]);
                    }
                }

                result[name] = value;
            }

            return result;
        }

        private object GetQuery(string name)
        {
            if (Queries.AllKeys.Contains(name))
            {
                return Queries[name];
            }

            if (!name.Contains("."))
            {
                return null;
            }

            return GetQueries(name)[name];
        }
    }
}

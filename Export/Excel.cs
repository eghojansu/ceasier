using Ceasier.Utils;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Web;

namespace Ceasier.Export
{
    public class Excel<T>
    {
        private readonly List<ExcelColumn> Columns = new List<ExcelColumn>();
        private readonly IEnumerable<T> Data;
        private readonly string Filename;
        private readonly string WorksheetName;

        public Excel(IEnumerable<T> data) : this(data, null)
        {
        }

        public Excel(IEnumerable<T> data, string filename): this(data, filename, null)
        {
        }

        public Excel(IEnumerable<T> data, string filename, string worksheet)
        {
            Data = data;
            Filename = filename ?? "data";
            WorksheetName = worksheet ?? "Worksheet";
        }

        public string SaveAs => $"{Filename}-{DateTime.Now:yyyyMMdd-HHmmss}.xlsx";

        public Excel<T> AddDate(string name)
        {
            return AddDate(name, null);
        }

        public Excel<T> AddDate(string name, string text)
        {
            return AddColumn(name, text, "dd/mm/yyyy");
        }

        public Excel<T> AddColumn(string name)
        {
            return AddColumn(name, null);
        }

        public Excel<T> AddColumn(string name, string text)
        {
            return AddColumn(name, text, null);
        }

        public Excel<T> AddColumn(string name, string text, string format)
        {
            return AddColumn(new ExcelColumn() { Name = name, Text = text ?? Common.CaseTitle(name), Format = format });
        }

        public Excel<T> AddColumn(ExcelColumn column)
        {
            column.Column = CharSequence(Columns.Count + 1);
            Columns.Add(column);

            return this;
        }

        private ExcelPackage GetPackage()
        {
            var row = 1;
            var pck = new ExcelPackage();
            var ws = pck.Workbook.Worksheets.Add(WorksheetName);

            foreach (var column in Columns)
            {
                ws.Cells[$"{column.Column}{row}"].Value = column.Text;
            }

            foreach (var dt in Data)
            {
                row++;

                var t = dt.GetType();

                foreach (var column in Columns)
                {
                    var cell = $"{column.Column}{row}";
                    var value = t.GetProperty(column.Name).GetValue(dt);

                    if (!(value is DateTime d) || d != DateTime.MinValue)
                    {
                        ws.Cells[cell].Value = value;
                    }

                    if (!string.IsNullOrEmpty(column.Format))
                    {
                        ws.Cells[cell].Style.Numberformat.Format = column.Format;
                    }
                }
            }

            return pck;
        }

        public void Send(HttpResponseBase response)
        {
            using (var pck = GetPackage())
            {
                var ts = DateTime.Now.ToString("yyyyMMdd-HHmmss");

                response.ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
                response.AddHeader("content-disposition", $"attachment;  filename={SaveAs}");
                response.BinaryWrite(pck.GetAsByteArray());
            }
        }

        private static string CharSequence(int n)
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
    }

    public class Excel : Excel<object>
    {
        public Excel(IEnumerable<object> data) : this(data, null)
        {
        }

        public Excel(IEnumerable<object> data, string filename) : this(data, filename, null)
        {
        }

        public Excel(IEnumerable<object> data, string filename, string worksheet) : base(data, filename, worksheet)
        {
        }
    }

    public class ExcelColumn
    {
        public string Name { get; set; }
        public string Text { get; set; }
        public string Column { get; set; }
        public string Format { get; set; }
    }
}

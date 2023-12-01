using Ceasier.Utils;
using OfficeOpenXml;
using System;
using System.Collections.Generic;
using System.Web.Mvc;

namespace Ceasier.Export
{
    public class Excel
    {
        private readonly List<ExcelColumn> Columns = new List<ExcelColumn>();
        private readonly IEnumerable<object> Data;
        private readonly string Filename;
        private readonly string WorksheetName;

        public Excel(IEnumerable<object> data) : this(data, null)
        {
        }

        public Excel(IEnumerable<object> data, string filename): this(data, filename, null)
        {
        }

        public Excel(IEnumerable<object> data, string filename, string worksheet)
        {
            Data = data;
            Filename = filename ?? "data";
            WorksheetName = worksheet ?? "Worksheet";
        }

        public string SaveAs => $"{Filename}-{DateTime.Now:yyyyMMdd-HHmmss}.xlsx";

        public Excel AddDate(string name)
        {
            return AddDate(name, null);
        }

        public Excel AddDate(string name, string text)
        {
            return AddColumn(ExcelColumn.Date(name, text));
        }

        public Excel AddColumn(string name)
        {
            return AddColumn(name, null);
        }

        public Excel AddColumn(string name, string text)
        {
            return AddColumn(name, text, null);
        }

        public Excel AddColumn(string name, string text, string format)
        {
            return AddColumn(ExcelColumn.Create(name, text, format));
        }

        public Excel AddColumn(ExcelColumn column)
        {
            Columns.Add(column.FromSequence(Columns.Count + 1));

            return this;
        }

        public Excel AddColumns(params ExcelColumn[] columns)
        {
            foreach (var column in columns)
            {
                AddColumn(column);
            }

            return this;
        }

        public FileContentResult GetResult() => new FileContentResult(GetPackage().GetAsByteArray(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        {
            FileDownloadName = SaveAs
        };

        public ExcelPackage GetPackage()
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

        public static FileContentResult Download(string filename, ExcelColumn[] columns, IEnumerable<object> data) => new Excel(data, filename).AddColumns(columns).GetResult();
    }

    public class ExcelColumn
    {
        public string Name { get; set; }
        public string Text { get; set; }
        public string Column { get; set; }
        public string Format { get; set; }

        public ExcelColumn(string name, string text, string format)
        {
            Name = name;
            Text = text ?? Common.CaseTitle(name);
            Format = format;
        }

        public ExcelColumn FromSequence(int no)
        {
            Column = Common.CharSequence(no);

            return this;
        }

        public static ExcelColumn Create(string name, string text, string format) => new ExcelColumn(name, text, format);

        public static ExcelColumn Create(string name) => Create(name, null);

        public static ExcelColumn Create(string name, string text) => Create(name, text, null);

        public static ExcelColumn Date(string name) => Date(name, null);

        public static ExcelColumn Date(string name, string text) => Create(name, text, "dd/mm/yyyy");
    }
}

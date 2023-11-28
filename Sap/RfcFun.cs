using Ceasier.Utils;
using SAP.Middleware.Connector;
using System;
using System.Data;
using System.Linq;

namespace Ceasier.Sap
{
    public class RfcFun
    {
        public readonly RfcConfigParameters Params;
        public readonly RfcDestination Dest;
        public readonly IRfcFunction Fun;

        private IRfcTable _result;
        private IRfcStructure _resultFirst;
        private string _resultTable;
        private string _returnTable;
        private string _returnMessage;
        private string _returnMessageType;
        private string _messageType;
        private string _message;

        public RfcFun(string name, RfcConfigParameters param)
        {
            Dest = RfcDestinationManager.GetDestination(param);
            Fun = Dest.Repository.CreateFunction(name);
            Params = param;
        }

        public void SetResultTable(string resultTable) => _resultTable = resultTable;

        public void SetReturnTable(string returnTable) => SetReturnTable(returnTable, "MESSAGE", "TYPE");

        public void SetReturnTable(string returnTable, string returnMessage, string returnMessageType)
        {
            _returnTable = returnTable;
            _returnMessage = returnMessage;
            _returnMessageType = returnMessageType;
        }

        public IRfcTable Result
        {
            get
            {
                if (_result == null)
                {
                    if (_resultTable == null)
                    {
                        throw new Exception("Result table is not defined");
                    }

                    _result = GetTable(_resultTable);
                }

                return _result;
            }
        }

        public IRfcStructure ResultFirst
        {
            get
            {
                if (_resultFirst == null)
                {
                    foreach (var row in Result)
                    {
                        _resultFirst = row;

                        break;
                    }
                }

                return _resultFirst;
            }
        }

        public string Message => _message;

        public string MessageType => _messageType;

        public bool Success => string.IsNullOrEmpty(MessageType) || "S".Equals(MessageType);

        public DataTable ToDataTable(string table, RfcField[] fields)
        {
            var dt = new DataTable(table);
            var list = fields.ToList();

            list.ForEach(field => dt.Columns.Add(field.Column, field.Type));

            foreach (var row in Result)
            {
                var item = new object[fields.Length];
                var i = 0;

                list.ForEach(field => item[i++] = field.GetValue(row));

                dt.Rows.Add(item);
            }

            return dt;
        }

        public void Run()
        {
            RfcSessionManager.BeginContext(Dest);
            Fun.Invoke(Dest);
            RfcSessionManager.EndContext(Dest);

            ProcessReturn();
        }

        public IRfcTable GetTable(string table) => Fun.GetTable(table);

        public void SetArgs(object sets) => Common.ObjectValues(sets).ForEach(set => Fun.SetValue(set.Key, set.Value));

        public void ApplyArgs(string table, object sets)
        {
            IRfcTable args = Fun.GetTable(table);

            args.Clear();
            args.Append();

            Common.ObjectValues(sets).ForEach(set => args.SetValue(set.Key, set.Value));
        }

        private void ProcessReturn()
        {
            _result = null;
            _resultFirst = null;

            if (_returnTable == null)
            {
                return;
            }

            IRfcStructure row = null;

            try
            {
                var error = Fun.GetTable(_returnTable);

                foreach (var row1 in error)
                {
                    row = row1;
                }
            }
            catch
            {
                try
                {
                    row = Fun.GetStructure(_returnTable);
                }
                catch
                { }
            }

            if (row != null)
            {
                _message = row.GetString(_returnMessage);
                _messageType = row.GetString(_returnMessageType);
            }
        }
    }
}

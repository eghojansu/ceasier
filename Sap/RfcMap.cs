using System;

namespace Ceasier.Sap
{
    public class RfcMap
    {
        public readonly string Name;
        public readonly string Profile;
        public readonly string Actor;

        public string Mapped => $"{Name}:{Profile}:{Actor}";

        public RfcMap(string name, string profile, string actor)
        {
            Name = name;
            Profile = profile;
            Actor = actor;
        }

        public RfcMap(string value)
        {
            var maps = value.Split(':');

            if (maps.Length < 3)
            {
                throw new Exception($"Unable mapping from value: {value}");
            }

            Name = maps[0];
            Profile = maps[1];
            Actor = maps[2];
        }
    }
}
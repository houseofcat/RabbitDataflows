﻿using System.Collections.Generic;

namespace HouseofCat.Data.Database
{
    public class OdbcOptions
    {
        public string Driver { get; set; }
        public bool UseDataSourceName { get; set; }

        public Dictionary<string, object> Properties { get; set; } = new Dictionary<string, object>();
    }
}

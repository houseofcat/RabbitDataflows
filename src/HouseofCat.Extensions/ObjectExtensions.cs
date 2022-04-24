﻿using System;
using System.Collections.Generic;
using System.Text;

namespace HouseofCat.Extensions
{
    public static class ObjectExtensions
    {
        private static readonly int _syncBlockSize = 4;
        private static readonly int _methodTableReferenceSize = 4;
        private static readonly int _lengthSize = 4;

        public static long GetByteCount(this object input)
        {
            if (input == null) return 0;
            var type = input.GetType();

            if (_primitiveTypeSizes.ContainsKey(type))
            {
                return _primitiveTypeSizes[type];
            }

            if (_primitiveArrayTypeMultiplier.ContainsKey(type))
            {
                return (_primitiveArrayTypeMultiplier[type] * ((Array)input).Length)
                    + _syncBlockSize
                    + _methodTableReferenceSize
                    + _lengthSize;
            }

            return input switch
            {
                string stringy => Encoding.Unicode.GetByteCount(stringy),
                _ => 0,
            };
        }

        private static readonly Dictionary<Type, int> _primitiveArrayTypeMultiplier = new Dictionary<Type, int>
        {
            { typeof(sbyte[]),    sizeof(sbyte)     },
            { typeof(byte[]),     sizeof(byte)      },
            { typeof(bool[]),     sizeof(bool)      },
            { typeof(short[]),    sizeof(short)     },
            { typeof(ushort[]),   sizeof(ushort)    },
            { typeof(int[]),      sizeof(int)       },
            { typeof(uint[]),     sizeof(uint)      },
            { typeof(long[]),     sizeof(long)      },
            { typeof(ulong[]),    sizeof(ulong)     },
            { typeof(float[]),    sizeof(float)     },
            { typeof(double[]),   sizeof(double)    },
            { typeof(decimal[]),  sizeof(decimal)   }
        };

        private static readonly Dictionary<Type, int> _primitiveTypeSizes = new Dictionary<Type, int>
        {
            { typeof(sbyte),    sizeof(sbyte)   },
            { typeof(byte),     sizeof(byte)    },
            { typeof(bool),     sizeof(bool)    },
            { typeof(short),    sizeof(short)   },
            { typeof(ushort),   sizeof(ushort)  },
            { typeof(char),     sizeof(char)    },
            { typeof(int),      sizeof(int)     },
            { typeof(uint),     sizeof(uint)    },
            { typeof(long),     sizeof(long)    },
            { typeof(ulong),    sizeof(ulong)   },
            { typeof(float),    sizeof(float)   },
            { typeof(double),   sizeof(double)  },
            { typeof(decimal),  sizeof(decimal) }
        };
    }
}

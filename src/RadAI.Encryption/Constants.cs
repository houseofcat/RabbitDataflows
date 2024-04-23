﻿using System.Collections.Generic;

namespace RadAI.Encryption;

public static class Constants
{
    public static class Aes
    {
        public const int MacBitSize = 128;
        public const int NonceSize = 12;

        public static HashSet<int> ValidKeySizes = new HashSet<int> { 16, 24, 32 };
    }
}

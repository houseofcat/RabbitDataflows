﻿using RadAI.Utilities.Errors;
using System;
using System.Collections.Generic;

namespace RadAI.Utilities;

public static class Streaming
{
    public static async IAsyncEnumerable<string> StreamOutStringsAsync(string fileNamePath)
    {
        Guard.AgainstNull(fileNamePath, nameof(fileNamePath));
        if (!System.IO.File.Exists(fileNamePath)) throw new ArgumentException($"{fileNamePath} is not a valid file path.");

        using var streamReader = System.IO.File.OpenText(fileNamePath);
        string currentLine;

        while ((currentLine = await streamReader.ReadLineAsync().ConfigureAwait(false)) != null)
        {
            if (!string.IsNullOrWhiteSpace(currentLine))
            {
                yield return currentLine;
            }
        }
    }
}

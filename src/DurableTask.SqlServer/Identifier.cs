// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer
{
    using System;
    using System.Text;

    static class Identifier
    {
        public static string Escape(string value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }

            if (value == "")
            {
                throw new ArgumentException("Value cannot be empty.", nameof(value));
            }

            StringBuilder builder = new StringBuilder();

            builder.Append('[');
            foreach (char c in value)
            {
                if (c == ']')
                {
                    builder.Append(']');
                }

                builder.Append(c);
            }
            builder.Append(']');

            return builder.ToString();
        }
    }
}

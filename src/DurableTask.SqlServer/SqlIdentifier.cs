// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer
{
    using System;
    using System.Text;

    static class SqlIdentifier
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

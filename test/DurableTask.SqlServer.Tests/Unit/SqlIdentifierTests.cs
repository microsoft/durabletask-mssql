// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See LICENSE in the project root for license information.

namespace DurableTask.SqlServer.Tests.Unit
{
    using Xunit;

    public class SqlIdentifierTests
    {
        [Theory]
        [InlineData("  ", "[  ]")]
        [InlineData("foo", "[foo]")]
        [InlineData("foo]bar", "[foo]]bar]")]
        [InlineData("foo\"bar", "[foo\"bar]")]
        [InlineData("𐊗𐊕𐊐𐊎𐊆𐊍𐊆", "[𐊗𐊕𐊐𐊎𐊆𐊍𐊆]")]
        [InlineData("DurableDB; DROP TABLE ImportantData", "[DurableDB; DROP TABLE ImportantData]")]
        public void EscapeSqlIdentifiers(string input, string expected)
        {
            Assert.Equal(expected, SqlIdentifier.Escape(input));
        }
    }
}

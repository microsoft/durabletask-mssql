// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.SqlServer.Tests.Unit
{
    using System;
    using System.Linq;
    using System.Reflection;
    using Microsoft.Data.SqlClient;
    using Xunit;

    public class SqlUtilsTransientTests
    {
        // Azure SQL: "Reason: An instance-specific error occurred while establishing a connection to SQL Server.
        // Connection was denied since Deny Public Network Access is set to Yes." Not transient by default.
        const int DenyPublicNetworkAccess = 47073;

        [Theory]
        [InlineData(1205)]  // deadlock
        [InlineData(40613)] // database not currently available
        [InlineData(10054)] // transport-level error
        public void BuiltInErrorNumbersAreTransient(int errorNumber)
        {
            SqlException e = CreateSqlException(errorNumber);

            Assert.True(SqlUtils.IsTransient(e));
            Assert.True(SqlUtils.IsTransient(e, Array.Empty<int>()));
        }

        [Fact]
        public void UnknownErrorNumberIsNotTransientByDefault()
        {
            SqlException e = CreateSqlException(DenyPublicNetworkAccess);

            Assert.False(SqlUtils.IsTransient(e));
            Assert.False(SqlUtils.IsTransient(e, additionalTransientErrorNumbers: null));
            Assert.False(SqlUtils.IsTransient(e, new[] { 50000, 12345 }));
        }

        [Fact]
        public void ConfiguredErrorNumberIsTransient()
        {
            SqlException e = CreateSqlException(DenyPublicNetworkAccess);

            Assert.True(SqlUtils.IsTransient(e, new[] { DenyPublicNetworkAccess }));
        }

        [Fact]
        public void ConfiguredErrorNumbersApplyToAnyErrorInTheCollection()
        {
            SqlException e = CreateSqlException(50000, DenyPublicNetworkAccess);

            Assert.False(SqlUtils.IsTransient(e));
            Assert.True(SqlUtils.IsTransient(e, new[] { DenyPublicNetworkAccess }));
        }

        [Fact]
        public void NonSqlExceptionsAreUnaffected()
        {
            int[] extra = new[] { DenyPublicNetworkAccess };

            Assert.True(SqlUtils.IsTransient(new TimeoutException(), extra));
            Assert.False(SqlUtils.IsTransient(new InvalidOperationException(), extra));
        }

        // SqlError, SqlErrorCollection and SqlException have no public constructors,
        // so build them through their internal APIs.
        static SqlException CreateSqlException(params int[] errorNumbers)
        {
            const BindingFlags NonPublicInstance = BindingFlags.NonPublic | BindingFlags.Instance;

            var errors = (SqlErrorCollection)Activator.CreateInstance(typeof(SqlErrorCollection), nonPublic: true)!;
            MethodInfo add = typeof(SqlErrorCollection).GetMethod("Add", NonPublicInstance)!;

            ConstructorInfo errorCtor = typeof(SqlError)
                .GetConstructors(NonPublicInstance)
                .Where(c => c.GetParameters().FirstOrDefault()?.ParameterType == typeof(int))
                .OrderBy(c => c.GetParameters().Length)
                .First();

            foreach (int number in errorNumbers)
            {
                object?[] args = errorCtor.GetParameters()
                    .Select((p, i) => i == 0
                        ? number
                        : p.ParameterType == typeof(string) ? $"Simulated SQL error {number}"
                        : p.ParameterType.IsValueType ? Activator.CreateInstance(p.ParameterType)
                        : null)
                    .ToArray();
                add.Invoke(errors, new[] { errorCtor.Invoke(args) });
            }

            MethodInfo create = typeof(SqlException).GetMethod(
                "CreateException",
                BindingFlags.NonPublic | BindingFlags.Static,
                binder: null,
                new[] { typeof(SqlErrorCollection), typeof(string) },
                modifiers: null)!;
            return (SqlException)create.Invoke(null, new object[] { errors, "16.0" })!;
        }
    }
}

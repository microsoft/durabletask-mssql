#if NET462 // These .NET Standard 2.1 methods are not available in .NET 4.x
namespace DurableTask.SqlServer
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Text;
    using Microsoft.Data.SqlClient;
    using System.Threading;

    public static class NetFxCompat
    {
        public static int GetInt32(this DbDataReader reader, string columnName)
        {
            int ordinal = reader.GetOrdinal(columnName);
            return reader.GetInt32(ordinal);
        }

        public static string GetString(this DbDataReader reader, string columnName)
        {
            int ordinal = reader.GetOrdinal(columnName);
            return reader.GetString(ordinal);
        }

        public static Task CloseAsync(this DbConnection connection)
        {
            connection.Close();
            return Task.CompletedTask;
        }

        public static Task<DbTransaction> BeginTransactionAsync(this DbConnection connection)
        {
            // BeginTransactionAsync is not available in the .NET Framework
            return Task.FromResult(connection.BeginTransaction());
        }

        public static Task CommitAsync(this DbTransaction transaction)
        {
            transaction.Commit();
            return Task.CompletedTask;
        }

        public static Task RollbackAsync(this DbTransaction transaction)
        {
            transaction.Rollback();
            return Task.CompletedTask;
        }

        public static IEnumerable<T> Append<T>(this IEnumerable<T> source, T item)
        {
            if (source is ICollection<T> collection)
            {
                collection.Add(item);
                return collection;
            }
            else
            {
                List<T> list = source.ToList();
                list.Add(item);
                return list;
            }
        }

        public static bool TryAdd<TKey, TValue>(this IDictionary<TKey, TValue> dict, TKey key, TValue value)
        {
            if (!dict.ContainsKey(key))
            {
                dict.Add(key, value);
                return true;
            }

            return false;
        }

        // https://stackoverflow.com/questions/57047174/wheres-deconstruct-method-of-keyvaluepair-struct
        public static void Deconstruct<TKey, TValue>(this KeyValuePair<TKey, TValue> pair, out TKey key, out TValue value)
        {
            key = pair.Key;
            value = pair.Value;
        }
    }
}

// https://www.strathweb.com/2019/11/using-async-disposable-and-async-enumerable-in-frameworks-older-than-net-core-3-0/
namespace System
{
    using System.Threading.Tasks;

    public interface IAsyncDisposable
    {
        ValueTask DisposeAsync();
    }
}
#endif

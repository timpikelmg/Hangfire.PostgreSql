// This file is part of Hangfire.PostgreSql.
// Copyright © 2014 Frank Hommers <http://hmm.rs/Hangfire.PostgreSql>.
// 
// Hangfire.PostgreSql is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.PostgreSql  is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.PostgreSql. If not, see <http://www.gnu.org/licenses/>.
//
// This work is based on the work of Sergey Odinokov, author of 
// Hangfire. <http://hangfire.io/>
//   
//    Special thanks goes to him.

using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using Dapper;
using Hangfire.Logging;
using Hangfire.PostgreSql.Utils;
using Hangfire.Storage;

namespace Hangfire.PostgreSql
{
  public class PostgreSqlTransactionJob : IFetchedJob
  {
    // We have a keep alive to ensure that proxies or pg_bouncer don't close this as
    // an idle connection
    private static readonly TimeSpan _keepAliveInterval = TimeSpan.FromMinutes(1);
    private readonly object _lockObject = new();

    private readonly ILog _logger = LogProvider.GetLogger(typeof(PostgreSqlTransactionJob));

    private readonly PostgreSqlStorage _storage;

    private readonly Timer _timer;
    private readonly IDbTransaction _transaction;
    private IDbConnection _connection;
    private bool _disposed;

    public PostgreSqlTransactionJob(
      PostgreSqlStorage storage,
      IDbConnection connection,
      IDbTransaction transaction,
      string jobId,
      string queue)
    {
      _storage = storage ?? throw new ArgumentNullException(nameof(storage));
      _connection = connection ?? throw new ArgumentNullException(nameof(connection));
      _transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));

      JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
      Queue = queue ?? throw new ArgumentNullException(nameof(queue));

      if (!_storage.IsExistingConnection(_connection))
      {
        _timer = new Timer(ExecuteKeepAliveQuery, null, _keepAliveInterval, _keepAliveInterval);
      }
    }

    public string Queue { get; }
    public string JobId { get; }

    public void RemoveFromQueue()
    {
      lock (_lockObject)
      {
        _transaction.Commit();
      }
    }

    public void Requeue()
    {
      lock (_lockObject)
      {
        _transaction.Rollback();
      }
    }

    public void Dispose()
    {
      if (_disposed)
      {
        return;
      }

      _disposed = true;

      // Timer callback may be invoked after the Dispose method call,
      // so we are using lock to avoid unsynchronized calls.
      lock (_lockObject)
      {
        _timer?.Dispose();
        _transaction.Dispose();
        _storage.ReleaseConnection(_connection);
        _connection = null;
      }
    }

    private void ExecuteKeepAliveQuery(object obj)
    {
      lock (_lockObject)
      {
        try
        {
          _connection?.Execute("SELECT 1;", transaction: _transaction);
        }
        catch (Exception ex) when (ex.IsCatchableExceptionType())
        {
          // Connection was closed. So we can't continue to send
          // keep-alive queries. Unlike for distributed locks,
          // there is no any caveats of having this issue for
          // queues, because Hangfire guarantees only the "at least
          // once" processing.
        }
      }
    }
  }
}
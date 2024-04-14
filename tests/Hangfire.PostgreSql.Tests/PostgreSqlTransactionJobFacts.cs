using System;
using System.Data;
using Hangfire.PostgreSql.Tests.Utils;
using Moq;
using Xunit;

// ReSharper disable AssignNullToNotNullAttribute

namespace Hangfire.PostgreSql.Tests
{
  public class PostgreSqlTransactionJobFacts
  {
    private const string JobId = "id";
    private const string Queue = "queue";

    private readonly Mock<IDbConnection> _connection;
    private readonly Mock<IDbTransaction> _transaction;
    private readonly PostgreSqlStorage _storage;

    public PostgreSqlTransactionJobFacts()
    {
      _connection = new Mock<IDbConnection>();
      _transaction = new Mock<IDbTransaction>();
      _storage = new PostgreSqlStorage(ConnectionUtils.GetDefaultConnectionFactory());
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenStorageIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
        () => new PostgreSqlTransactionJob(null, _connection.Object, _transaction.Object, JobId, Queue));

      Assert.Equal("storage", exception.ParamName);
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenConnectionIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
        () => new PostgreSqlTransactionJob(_storage, null, _transaction.Object, JobId, Queue));

      Assert.Equal("connection", exception.ParamName);
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenTransactionIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
        () => new PostgreSqlTransactionJob(_storage, _connection.Object, null, JobId, Queue));

      Assert.Equal("transaction", exception.ParamName);
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenJobIdIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
        () => new PostgreSqlTransactionJob(_storage, _connection.Object, _transaction.Object, null, Queue));

      Assert.Equal("jobId", exception.ParamName);
    }

    [Fact]
    public void Ctor_ThrowsAnException_WhenQueueIsNull()
    {
      ArgumentNullException exception = Assert.Throws<ArgumentNullException>(
        () => new PostgreSqlTransactionJob(_storage, _connection.Object, _transaction.Object, JobId, null));

      Assert.Equal("queue", exception.ParamName);
    }

    [Fact]
    public void Ctor_CorrectlySets_AllInstanceProperties()
    {
      PostgreSqlTransactionJob fetchedJob = new PostgreSqlTransactionJob(_storage, _connection.Object, _transaction.Object, JobId, Queue);

      Assert.Equal(JobId, fetchedJob.JobId);
      Assert.Equal(Queue, fetchedJob.Queue);
    }

    [Fact]
    [CleanDatabase]
    public void RemoveFromQueue_CommitsTheTransaction()
    {
      // Arrange
      PostgreSqlTransactionJob processingJob = CreateFetchedJob("1", "default");

      // Act
      processingJob.RemoveFromQueue();

      // Assert
      _transaction.Verify(x => x.Commit());
    }

    [Fact]
    [CleanDatabase]
    public void Requeue_RollsbackTheTransaction()
    {
      // Arrange
      PostgreSqlTransactionJob processingJob = CreateFetchedJob("1", "default");

      // Act
      processingJob.Requeue();

      // Assert
      _transaction.Verify(x => x.Rollback());
    }

    [Fact]
    [CleanDatabase]
    public void Dispose_DisposesTheTransactionAndConnection()
    {
      PostgreSqlTransactionJob processingJob = CreateFetchedJob("1", "queue");

      // Act
      processingJob.Dispose();

      // Assert
      _transaction.Verify(x => x.Dispose());
      _connection.Verify(x => x.Dispose());
    }

    private PostgreSqlTransactionJob CreateFetchedJob(string jobId, string queue)
    {
      return new PostgreSqlTransactionJob(_storage, _connection.Object, _transaction.Object, jobId, queue);
    }
  }
}

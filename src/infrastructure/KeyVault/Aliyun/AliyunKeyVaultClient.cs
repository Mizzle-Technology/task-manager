#nullable enable
using System.Text;
using AlibabaCloud.SDK.Kms20160120;
using AlibabaCloud.SDK.Kms20160120.Models;
using AlibabaCloud.TeaUtil.Models;
using infrastructure.Configuration.KeyVault;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using Tea;

namespace infrastructure.KeyVault.Aliyun;

public class AliyunKeyVaultClient : IKeyVaultClient
{
    private readonly Client _client;
    private readonly IMemoryCache _cache;
    private readonly TimeSpan _cacheTtl;
    private readonly ILogger<AliyunKeyVaultClient> _logger;
    private readonly AsyncRetryPolicy<string> _retryPolicy;

    public AliyunKeyVaultClient(
        IOptions<KeyVaultConfiguration> options,
        IMemoryCache cache,
        IAliyunKmsClientFactory clientFactory,
        ILogger<AliyunKeyVaultClient> logger
    )
    {
        var kvConfig = options?.Value ?? throw new ArgumentNullException(nameof(options));
        if (string.IsNullOrWhiteSpace(kvConfig.Endpoint))
            throw new ArgumentException(
                "KeyVaultConfiguration.Endpoint is required",
                nameof(options)
            );

        _client =
            clientFactory?.CreateClient(kvConfig)
            ?? throw new ArgumentNullException(nameof(clientFactory));
        _cache = cache ?? throw new ArgumentNullException(nameof(cache));
        // Ensure TTL is positive, default to 60 seconds if not
        int ttl = kvConfig.CacheTtlSeconds > 0 ? kvConfig.CacheTtlSeconds : 60;
        _cacheTtl = TimeSpan.FromSeconds(ttl);
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        _retryPolicy = Policy<string>
            .Handle<TeaException>()
            .Or<InvalidOperationException>()
            .WaitAndRetryAsync(
                retryCount: 3,
                sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                onRetry: (outcome, ts, count, ctx) =>
                {
                    var exception = outcome.Exception;
                    if (exception != null)
                    {
                        _logger.LogWarning(exception, "Retry {RetryCount} due to error", count);
                    }
                    else
                    {
                        _logger.LogWarning("Retry {RetryCount} without exception", count);
                    }
                }
            );
    }

    private static RuntimeOptions CreateRuntimeOptions() => new();

    public async Task<string> GetSecretAsync(
        string secretName,
        CancellationToken cancellationToken = default
    )
    {
        if (string.IsNullOrWhiteSpace(secretName))
            throw new ArgumentException("secretName is required", nameof(secretName));

        if (_cache.TryGetValue(secretName, out string? cached))
        {
            _logger.LogDebug("Cache hit for secret {SecretName}", secretName);
            return cached!;
        }

        try
        {
            var secret = await _retryPolicy
                .ExecuteAsync(
                    async ct =>
                    {
                        var req = new GetSecretValueRequest { SecretName = secretName };
                        var runtime = CreateRuntimeOptions();
                        var resp = await _client
                            .GetSecretValueWithOptionsAsync(req, runtime)
                            .WaitAsync(ct)
                            .ConfigureAwait(false);
                        return resp.Body.SecretData
                            ?? throw new InvalidOperationException(
                                $"SecretData was null for {secretName}"
                            );
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);

            _cache.Set(secretName, secret, _cacheTtl);
            return secret;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error retrieving secret {SecretName}, returning stale if available",
                secretName
            );
            if (_cache.TryGetValue(secretName, out string? stale))
                return stale!;
            throw;
        }
    }

    public async Task<string> EncryptAsync(
        string keyId,
        string plaintext,
        CancellationToken cancellationToken = default
    )
    {
        if (string.IsNullOrWhiteSpace(keyId))
            throw new ArgumentException("keyId is required", nameof(keyId));
        if (string.IsNullOrWhiteSpace(plaintext))
            throw new ArgumentException("plaintext is required", nameof(plaintext));

        var blob = Convert.ToBase64String(Encoding.UTF8.GetBytes(plaintext));
        return await ExecuteWithRetryAsync(
                async ct =>
                {
                    var req = new EncryptRequest { KeyId = keyId, Plaintext = blob };
                    var runtime = CreateRuntimeOptions();
                    var resp = await _client
                        .EncryptWithOptionsAsync(req, runtime)
                        .WaitAsync(ct)
                        .ConfigureAwait(false);
                    return resp.Body.CiphertextBlob
                        ?? throw new InvalidOperationException("CiphertextBlob was null");
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    public async Task<string> DecryptAsync(
        string ciphertextBase64,
        CancellationToken cancellationToken = default
    )
    {
        if (string.IsNullOrWhiteSpace(ciphertextBase64))
            throw new ArgumentException("ciphertextBase64 is required", nameof(ciphertextBase64));

        return await ExecuteWithRetryAsync(
                async ct =>
                {
                    var req = new DecryptRequest { CiphertextBlob = ciphertextBase64 };
                    var runtime = CreateRuntimeOptions();
                    var resp = await _client
                        .DecryptWithOptionsAsync(req, runtime)
                        .WaitAsync(ct)
                        .ConfigureAwait(false);
                    var bytes = Convert.FromBase64String(resp.Body.Plaintext);
                    return Encoding.UTF8.GetString(bytes);
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }

    private async Task<string> ExecuteWithRetryAsync(
        Func<CancellationToken, Task<string>> operation,
        CancellationToken cancellationToken
    )
    {
        try
        {
            return await _retryPolicy
                .ExecuteAsync(operation, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TeaException te)
        {
            _logger.LogError(te, "Aliyun KMS operation failed");
            throw new InvalidOperationException($"Aliyun KMS operation failed: {te.Message}", te);
        }
    }
}

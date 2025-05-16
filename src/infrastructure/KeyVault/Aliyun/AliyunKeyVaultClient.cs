using System.Text;
using AlibabaCloud.OpenApiClient.Models;
using AlibabaCloud.SDK.Kms20160120;
using AlibabaCloud.SDK.Kms20160120.Models;
using AlibabaCloud.TeaUtil.Models;
using infrastructure.Configuration.KeyVault;
using Microsoft.Extensions.Options;
using Tea;

namespace infrastructure.KeyVault.Aliyun;

public class AliyunKeyVaultClient : IKeyVaultClient
{
    private readonly string _defaultKeyId;
    private readonly Client _client;

    public AliyunKeyVaultClient(IOptions<KeyVaultConfiguration> options)
    {
        var kvConfig = options.Value;
        if (string.IsNullOrWhiteSpace(kvConfig.Endpoint))
            throw new ArgumentException(
                "KeyVaultConfiguration.Endpoint is required",
                nameof(options)
            );
        _defaultKeyId = kvConfig.DefaultKeyId ?? string.Empty;
        // Read credentials from environment
        var accessKeyId =
            Environment.GetEnvironmentVariable("ALIBABA_CLOUD_ACCESS_KEY_ID")
            ?? throw new InvalidOperationException(
                "Missing environment variable ALIBABA_CLOUD_ACCESS_KEY_ID"
            );
        var accessKeySecret =
            Environment.GetEnvironmentVariable("ALIBABA_CLOUD_ACCESS_KEY_SECRET")
            ?? throw new InvalidOperationException(
                "Missing environment variable ALIBABA_CLOUD_ACCESS_KEY_SECRET"
            );
        var config = new Config
        {
            AccessKeyId = accessKeyId,
            AccessKeySecret = accessKeySecret,
            Endpoint = kvConfig.Endpoint,
        };
        _client = new Client(config);
    }

    public async Task<string> GetSecretAsync(
        string secretName,
        CancellationToken cancellationToken = default
    )
    {
        if (string.IsNullOrWhiteSpace(secretName))
            throw new ArgumentException("secretName is required", nameof(secretName));
        var request = new GetSecretValueRequest { SecretName = secretName };
        try
        {
            var runtime = new RuntimeOptions();
            var response = await _client
                .GetSecretValueWithOptionsAsync(request, runtime)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            return response.Body.SecretData
                ?? throw new InvalidOperationException($"SecretData was null for {secretName}");
        }
        catch (TeaException e)
        {
            throw new InvalidOperationException($"Aliyun KMS error: {e.Message}", e);
        }
    }

    /// <inheritdoc />
    public async Task<string> EncryptAsync(
        string keyId,
        string plaintext,
        CancellationToken cancellationToken = default
    )
    {
        keyId = string.IsNullOrWhiteSpace(keyId) ? _defaultKeyId : keyId;
        if (string.IsNullOrWhiteSpace(keyId))
            throw new ArgumentException("keyId is required", nameof(keyId));
        if (string.IsNullOrWhiteSpace(plaintext))
            throw new ArgumentException("plaintext is required", nameof(plaintext));
        var request = new EncryptRequest
        {
            KeyId = keyId,
            Plaintext = Convert.ToBase64String(Encoding.UTF8.GetBytes(plaintext)),
        };
        try
        {
            var runtime = new RuntimeOptions();
            var response = await _client
                .EncryptWithOptionsAsync(request, runtime)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            return response.Body.CiphertextBlob
                ?? throw new InvalidOperationException("CiphertextBlob was null");
        }
        catch (TeaException e)
        {
            throw new InvalidOperationException($"Aliyun KMS encrypt error: {e.Message}", e);
        }
    }

    /// <inheritdoc />
    public async Task<string> DecryptAsync(
        string ciphertextBase64,
        CancellationToken cancellationToken = default
    )
    {
        if (string.IsNullOrWhiteSpace(ciphertextBase64))
            throw new ArgumentException("ciphertextBase64 is required", nameof(ciphertextBase64));
        var request = new DecryptRequest { CiphertextBlob = ciphertextBase64 };
        try
        {
            var runtime = new RuntimeOptions();
            var response = await _client
                .DecryptWithOptionsAsync(request, runtime)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            var bytes = Convert.FromBase64String(response.Body.Plaintext);
            return Encoding.UTF8.GetString(bytes);
        }
        catch (TeaException e)
        {
            throw new InvalidOperationException($"Aliyun KMS decrypt error: {e.Message}", e);
        }
    }
}

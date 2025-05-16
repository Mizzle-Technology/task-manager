namespace infrastructure.Configuration.KeyVault;

/// <summary>
/// Configuration settings for Key Vault (KMS) clients.
/// </summary>
public class KeyVaultConfiguration
{
    /// <summary>
    /// The KMS endpoint (e.g. "kms.cn-hangzhou.aliyuncs.com").
    /// </summary>
    public string Endpoint { get; set; } = "kms.cn-hangzhou.aliyuncs.com";

    /// <summary>
    /// The default key identifier to use for encryption/decryption.
    /// </summary>
    public string DefaultKeyId { get; set; } = string.Empty;
}

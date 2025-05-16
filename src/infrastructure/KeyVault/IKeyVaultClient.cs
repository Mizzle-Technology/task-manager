namespace infrastructure.KeyVault;

using System.Threading;
using System.Threading.Tasks;

public interface IKeyVaultClient
{
    /// <summary>
    /// Retrieves a secret value by name.
    /// </summary>
    /// <param name="secretName">The name of the secret to retrieve.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The secret value.</returns>
    Task<string> GetSecretAsync(string secretName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Encrypts plaintext using the specified key.
    /// </summary>
    Task<string> EncryptAsync(
        string keyId,
        string plaintext,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Decrypts the base64-encoded ciphertext and returns the plaintext.
    /// </summary>
    Task<string> DecryptAsync(
        string ciphertextBase64,
        CancellationToken cancellationToken = default
    );
}

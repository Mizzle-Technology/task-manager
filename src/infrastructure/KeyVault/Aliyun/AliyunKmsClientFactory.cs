using System;
using AlibabaCloud.OpenApiClient.Models;
using AlibabaCloud.SDK.Kms20160120;
using infrastructure.Configuration.KeyVault;

namespace infrastructure.KeyVault.Aliyun
{
    public interface IAliyunKmsClientFactory
    {
        Client CreateClient(KeyVaultConfiguration config);
    }

    public class AliyunKmsClientFactory : IAliyunKmsClientFactory
    {
        public Client CreateClient(KeyVaultConfiguration config)
        {
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

            var sdkConfig = new Config
            {
                AccessKeyId = accessKeyId,
                AccessKeySecret = accessKeySecret,
                Endpoint = config.Endpoint,
            };
            return new Client(sdkConfig);
        }
    }
}

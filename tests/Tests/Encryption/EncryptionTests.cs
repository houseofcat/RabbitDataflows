using HouseofCat.Encryption;
using HouseofCat.Hashing;
using HouseofCat.Hashing.Argon;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Encryption
{
    public class EncryptionTests
    {
        private readonly ITestOutputHelper _output;
        private readonly IHashingProvider _hashingProvider;
        private const string Passphrase = "SuperNintendoHadTheBestZelda";
        private const string Salt = "SegaGenesisIsTheBestConsole";
        private static byte[] _data = new byte[] { 0xFF, 0x00, 0xAA, 0xFF, 0x00, 0x00, 0xFF, 0xAA, 0x00, 0xFF, 0x00, 0xFF };

        public EncryptionTests(ITestOutputHelper output)
        {
            _output = output;
            _hashingProvider = new Argon2ID_HashingProvider();
        }

        [Fact]
        public void Aes256_GCM()
        {
            var hashKey = _hashingProvider.GetHashKey(Passphrase, Salt, 32);

            _output.WriteLine(Encoding.UTF8.GetString(hashKey));
            _output.WriteLine($"HashKey: {Encoding.UTF8.GetString(hashKey)}");

            var encryptionProvider = new AesGcmEncryptionProvider(hashKey);

            var encryptedData = encryptionProvider.Encrypt(_data);
            _output.WriteLine($"Encrypted: {Encoding.UTF8.GetString(encryptedData)}");

            var decryptedData = encryptionProvider.Decrypt(encryptedData);
            _output.WriteLine($"Data: {Encoding.UTF8.GetString(_data)}");
            _output.WriteLine($"Decrypted: {Encoding.UTF8.GetString(decryptedData)}");

            Assert.Equal(_data, decryptedData);
        }

        [Fact]
        public void Aes256_GCM_Stream()
        {
            var hashKey = _hashingProvider.GetHashKey(Passphrase, Salt, 32);

            _output.WriteLine(Encoding.UTF8.GetString(hashKey));
            _output.WriteLine($"HashKey: {Encoding.UTF8.GetString(hashKey)}");

            var encryptionProvider = new AesGcmEncryptionProvider(hashKey);
            var encryptedStream = encryptionProvider.Encrypt(new MemoryStream(_data));
            var decryptedStream = encryptionProvider.Decrypt(encryptedStream);

            Assert.Equal(_data, decryptedStream.ToArray());
        }

        [Fact]
        public async Task Aes256_GCM_StreamAsync()
        {
            var hashKey = await _hashingProvider
                .GetHashKeyAsync(Passphrase, Salt, 32)
                .ConfigureAwait(false);

            _output.WriteLine(Encoding.UTF8.GetString(hashKey));
            _output.WriteLine($"HashKey: {Encoding.UTF8.GetString(hashKey)}");

            var encryptionProvider = new AesGcmEncryptionProvider(hashKey);
            var encryptedStream = await encryptionProvider.EncryptAsync(new MemoryStream(_data));
            var decryptedStream = encryptionProvider.Decrypt(encryptedStream);

            Assert.Equal(_data, decryptedStream.ToArray());
        }

        [Fact]
        public void Aes192_GCM()
        {
            var hashKey = _hashingProvider.GetHashKey(Passphrase, Salt, 24);

            _output.WriteLine(Encoding.UTF8.GetString(hashKey));
            _output.WriteLine($"HashKey: {Encoding.UTF8.GetString(hashKey)}");

            var encryptionProvider = new AesGcmEncryptionProvider(hashKey);

            var encryptedData = encryptionProvider.Encrypt(_data);
            _output.WriteLine($"Encrypted: {Encoding.UTF8.GetString(encryptedData)}");

            var decryptedData = encryptionProvider.Decrypt(encryptedData);
            _output.WriteLine($"Data: {Encoding.UTF8.GetString(_data)}");
            _output.WriteLine($"Decrypted: {Encoding.UTF8.GetString(decryptedData)}");

            Assert.Equal(_data, decryptedData);
        }

        [Fact]
        public void Aes192_GCM_Stream()
        {
            var hashKey = _hashingProvider.GetHashKey(Passphrase, Salt, 24);

            _output.WriteLine(Encoding.UTF8.GetString(hashKey));
            _output.WriteLine($"HashKey: {Encoding.UTF8.GetString(hashKey)}");

            var encryptionProvider = new AesGcmEncryptionProvider(hashKey);
            var encryptedStream = encryptionProvider.Encrypt(new MemoryStream(_data));
            var decryptedStream = encryptionProvider.Decrypt(encryptedStream);

            Assert.Equal(_data, decryptedStream.ToArray());
        }

        [Fact]
        public async Task Aes192_GCM_StreamAsync()
        {
            var hashKey = await _hashingProvider
                .GetHashKeyAsync(Passphrase, Salt, 24)
                .ConfigureAwait(false);

            _output.WriteLine(Encoding.UTF8.GetString(hashKey));
            _output.WriteLine($"HashKey: {Encoding.UTF8.GetString(hashKey)}");

            var encryptionProvider = new AesGcmEncryptionProvider(hashKey);
            var encryptedStream = await encryptionProvider.EncryptAsync(new MemoryStream(_data));
            var decryptedStream = encryptionProvider.Decrypt(encryptedStream);

            Assert.Equal(_data, decryptedStream.ToArray());
        }

        [Fact]
        public void Aes128_GCM()
        {
            var hashKey = _hashingProvider.GetHashKey(Passphrase, Salt, 16);

            _output.WriteLine(Encoding.UTF8.GetString(hashKey));
            _output.WriteLine($"HashKey: {Encoding.UTF8.GetString(hashKey)}");

            var encryptionProvider = new AesGcmEncryptionProvider(hashKey);

            var encryptedData = encryptionProvider.Encrypt(_data);
            _output.WriteLine($"Encrypted: {Encoding.UTF8.GetString(encryptedData)}");

            var decryptedData = encryptionProvider.Decrypt(encryptedData);
            _output.WriteLine($"Data: {Encoding.UTF8.GetString(_data)}");
            _output.WriteLine($"Decrypted: {Encoding.UTF8.GetString(decryptedData)}");

            Assert.Equal(_data, decryptedData);
        }

        [Fact]
        public void Aes128_GCM_Stream()
        {
            var hashKey = _hashingProvider.GetHashKey(Passphrase, Salt, 16);

            _output.WriteLine(Encoding.UTF8.GetString(hashKey));
            _output.WriteLine($"HashKey: {Encoding.UTF8.GetString(hashKey)}");

            var encryptionProvider = new AesGcmEncryptionProvider(hashKey);
            var encryptedStream = encryptionProvider.Encrypt(new MemoryStream(_data));
            var decryptedStream = encryptionProvider.Decrypt(encryptedStream);

            Assert.Equal(_data, decryptedStream.ToArray());
        }

        [Fact]
        public async Task Aes128_GCM_StreamAsync()
        {
            var hashKey = await _hashingProvider
                .GetHashKeyAsync(Passphrase, Salt, 16)
                .ConfigureAwait(false);

            _output.WriteLine(Encoding.UTF8.GetString(hashKey));
            _output.WriteLine($"HashKey: {Encoding.UTF8.GetString(hashKey)}");

            var encryptionProvider = new AesGcmEncryptionProvider(hashKey);
            var encryptedStream = await encryptionProvider.EncryptAsync(new MemoryStream(_data));
            var decryptedStream = encryptionProvider.Decrypt(encryptedStream);

            Assert.Equal(_data, decryptedStream.ToArray());
        }

        [Fact]
        public async Task Aes256_StreamAsync()
        {
            // Arrange
            var hashKey = await _hashingProvider
                .GetHashKeyAsync(Passphrase, Salt, 32)
                .ConfigureAwait(false);

            var encryptionProvider = new AesStreamEncryptionProvider(hashKey);
            var message = "Hello World!";
            var fileNamePath = "./test_enc.txt";

            if (File.Exists(fileNamePath))
            { File.Delete(fileNamePath); }

            // Act
            using (var encFileStream = File.Open(fileNamePath, FileMode.OpenOrCreate))
            using (var cryptoStream1 = encryptionProvider.GetEncryptStream(encFileStream))
            {
                await cryptoStream1.WriteAsync(Encoding.UTF8.GetBytes(message));
                await cryptoStream1.FlushFinalBlockAsync();
            }

            var decryptedMessage = string.Empty;
            using (var decFileStream = File.Open(fileNamePath, FileMode.OpenOrCreate))
            using (var cryptoStream2 = encryptionProvider.GetDecryptStream(decFileStream))
            using (var streamReader = new StreamReader(cryptoStream2, Encoding.UTF8))
            {
                decryptedMessage = await streamReader.ReadToEndAsync();
            }

            // Assert
            _output.WriteLine($"Decrypted Text: {decryptedMessage}");
            Assert.Equal(message, decryptedMessage);
        }
    }
}

﻿using HouseofCat.Extensions;
using HouseofCat.Recyclable;
using HouseofCat.Utilities.Errors;
using Microsoft.Toolkit.HighPerformance;
using System;
using System.Buffers;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace HouseofCat.Encryption
{
    public class RecyclableAesGcmEncryptionProvider : IEncryptionProvider
    {
        public string Type { get; }

        private readonly RandomNumberGenerator _rng = RandomNumberGenerator.Create();
        private readonly ArrayPool<byte> _pool = ArrayPool<byte>.Shared;

        private readonly ReadOnlyMemory<byte> _key;

        public RecyclableAesGcmEncryptionProvider(ReadOnlyMemory<byte> key)
        {
            Guard.AgainstEmpty(key, nameof(key));

            if (!Constants.Aes.ValidKeySizes.Contains(key.Length)) throw new ArgumentException("Keysize is an invalid length.");
            _key = key;

            switch (_key.Length)
            {
                case 16: Type = "AESGCM-128"; break;
                case 24: Type = "AESGCM-192"; break;
                case 32: Type = "AESGCM-256"; break;
            }
        }

        public ArraySegment<byte> Encrypt(ReadOnlyMemory<byte> unencryptedData)
        {
            Guard.AgainstEmpty(unencryptedData, nameof(unencryptedData));

            using var aes = new AesGcm(_key.Span);

            // Slicing Version
            // Rented arrays sizes are minimums, not guarantees.
            // Need to perform extra work managing slices to keep the byte sizes correct but the memory allocations are lower by 200%
            var encryptedBytes = _pool.Rent(unencryptedData.Length);
            var tag = _pool.Rent(AesGcm.TagByteSizes.MaxSize); // MaxSize = 16
            var nonce = _pool.Rent(AesGcm.NonceByteSizes.MaxSize); // MaxSize = 12
            _rng.GetBytes(nonce, 0, AesGcm.NonceByteSizes.MaxSize);

            aes.Encrypt(
                nonce.AsSpan().Slice(0, AesGcm.NonceByteSizes.MaxSize),
                unencryptedData.Span,
                encryptedBytes.AsSpan().Slice(0, unencryptedData.Length),
                tag.AsSpan().Slice(0, AesGcm.TagByteSizes.MaxSize));

            // Prefix ciphertext with nonce and tag, since they are fixed length and it will simplify decryption.
            // Our pattern: Nonce Tag Cipher
            // Other patterns people use: Nonce Cipher Tag // couldn't find a solid source.
            var encryptedData = new byte[AesGcm.NonceByteSizes.MaxSize + AesGcm.TagByteSizes.MaxSize + unencryptedData.Length];
            Buffer.BlockCopy(nonce, 0, encryptedData, 0, AesGcm.NonceByteSizes.MaxSize);
            Buffer.BlockCopy(tag, 0, encryptedData, AesGcm.NonceByteSizes.MaxSize, AesGcm.TagByteSizes.MaxSize);
            Buffer.BlockCopy(encryptedBytes, 0, encryptedData, AesGcm.NonceByteSizes.MaxSize + AesGcm.TagByteSizes.MaxSize, unencryptedData.Length);

            _pool.Return(encryptedBytes);
            _pool.Return(tag);
            _pool.Return(nonce);

            return encryptedData;
        }

        public MemoryStream Encrypt(MemoryStream unencryptedStream, bool leaveStreamOpen = false)
        {
            Guard.AgainstNullOrEmpty(unencryptedStream, nameof(unencryptedStream));

            using var aes = new AesGcm(_key.Span);

            var length = (int)unencryptedStream.Length;
            (var buffer, var returnBuffer) = unencryptedStream.GetSafeBuffer(length);

            // Slicing Version
            // Rented arrays sizes are minimums, not guarantees.
            // Need to perform extra work managing slices to keep the byte sizes correct but the memory allocations are lower by 200%
            var encryptedBytes = _pool.Rent(length);
            var tag = _pool.Rent(AesGcm.TagByteSizes.MaxSize); // MaxSize = 16
            var nonce = _pool.Rent(AesGcm.NonceByteSizes.MaxSize); // MaxSize = 12
            _rng.GetBytes(nonce, 0, AesGcm.NonceByteSizes.MaxSize);

            aes.Encrypt(
                nonce.AsSpan().Slice(0, AesGcm.NonceByteSizes.MaxSize),
                buffer.AsSpan().Slice(0, length),
                encryptedBytes.AsSpan().Slice(0, length),
                tag.AsSpan().Slice(0, AesGcm.TagByteSizes.MaxSize));

            // Prefix ciphertext with nonce and tag, since they are fixed length and it will simplify decryption.
            // Our pattern: Nonce Tag Cipher
            // Other patterns people use: Nonce Cipher Tag // couldn't find a solid source.
            var encryptedStream = RecyclableManager.GetStream(
                nameof(RecyclableAesGcmEncryptionProvider),
                AesGcm.NonceByteSizes.MaxSize + AesGcm.TagByteSizes.MaxSize + length);

            using (var binaryWriter = new BinaryWriter(encryptedStream, Encoding.UTF8, true))
            {
                binaryWriter.Write(nonce, 0, AesGcm.NonceByteSizes.MaxSize);
                binaryWriter.Write(tag, 0, AesGcm.TagByteSizes.MaxSize);
                binaryWriter.Write(encryptedBytes, 0, length);
            }

            if (returnBuffer)
            { _pool.Return(buffer.Array); }

            _pool.Return(encryptedBytes);
            _pool.Return(tag);
            _pool.Return(nonce);

            encryptedStream.Seek(0, SeekOrigin.Begin);

            if (!leaveStreamOpen) { unencryptedStream.Close(); }

            return encryptedStream;
        }

        public async Task<MemoryStream> EncryptAsync(MemoryStream unencryptedStream, bool leaveStreamOpen = false)
        {
            Guard.AgainstNullOrEmpty(unencryptedStream, nameof(unencryptedStream));

            using var aes = new AesGcm(_key.Span);

            var length = (int)unencryptedStream.Length;
            (var buffer, var returnTobuffer) = await unencryptedStream.GetSafeBufferAsync(length);

            // Slicing Version
            // Rented arrays sizes are minimums, not guarantees.
            // Need to perform extra work managing slices to keep the byte sizes correct but the memory allocations are lower by 200%
            var encryptedBytes = _pool.Rent(length);
            var tag = _pool.Rent(AesGcm.TagByteSizes.MaxSize); // MaxSize = 16
            var nonce = _pool.Rent(AesGcm.NonceByteSizes.MaxSize); // MaxSize = 12
            _rng.GetBytes(nonce, 0, AesGcm.NonceByteSizes.MaxSize);

            aes.Encrypt(
                nonce.AsSpan().Slice(0, AesGcm.NonceByteSizes.MaxSize),
                buffer.Slice(0, length),
                encryptedBytes.AsSpan().Slice(0, length),
                tag.AsSpan().Slice(0, AesGcm.TagByteSizes.MaxSize));

            // Prefix ciphertext with nonce and tag, since they are fixed length and it will simplify decryption.
            // Our pattern: Nonce Tag Cipher
            // Other patterns people use: Nonce Cipher Tag // couldn't find a solid source.
            var encryptedStream = RecyclableManager.GetStream(
                nameof(RecyclableAesGcmEncryptionProvider),
                AesGcm.NonceByteSizes.MaxSize + AesGcm.TagByteSizes.MaxSize + length);

            using (var binaryWriter = new BinaryWriter(encryptedStream, Encoding.UTF8, true))
            {
                binaryWriter.Write(nonce, 0, AesGcm.NonceByteSizes.MaxSize);
                binaryWriter.Write(tag, 0, AesGcm.TagByteSizes.MaxSize);
                binaryWriter.Write(encryptedBytes, 0, length);
            }

            if (returnTobuffer)
            { _pool.Return(buffer.Array); }

            _pool.Return(encryptedBytes);
            _pool.Return(tag);
            _pool.Return(nonce);

            encryptedStream.Seek(0, SeekOrigin.Begin);

            if (!leaveStreamOpen) { unencryptedStream.Close(); }

            return encryptedStream;
        }

        public MemoryStream EncryptToStream(ReadOnlyMemory<byte> unencryptedData)
        {
            Guard.AgainstEmpty(unencryptedData, nameof(unencryptedData));

            return RecyclableManager.GetStream(nameof(RecyclableAesGcmEncryptionProvider), Encrypt(unencryptedData));
        }

        public ArraySegment<byte> Decrypt(ReadOnlyMemory<byte> encryptedData)
        {
            Guard.AgainstEmpty(encryptedData, nameof(encryptedData));

            using var aes = new AesGcm(_key.Span);

            // Slicing Version
            var nonce = encryptedData
                .Slice(0, AesGcm.NonceByteSizes.MaxSize)
                .Span;

            var tag = encryptedData
                .Slice(AesGcm.NonceByteSizes.MaxSize, AesGcm.TagByteSizes.MaxSize)
                .Span;

            var encryptedBytes = encryptedData
                .Slice(AesGcm.NonceByteSizes.MaxSize + AesGcm.TagByteSizes.MaxSize)
                .Span;

            var decryptedBytes = new byte[encryptedBytes.Length];

            aes.Decrypt(nonce, encryptedBytes, tag, decryptedBytes);

            return decryptedBytes;
        }

        public MemoryStream Decrypt(MemoryStream encryptedStream, bool leaveStreamOpen = false)
        {
            Guard.AgainstNullOrEmpty(encryptedStream, nameof(encryptedStream));

            if (encryptedStream.Position == encryptedStream.Length) { encryptedStream.Seek(0, SeekOrigin.Begin); }

            using var aes = new AesGcm(_key.Span);

            var encryptedByteLength = (int)encryptedStream.Length - AesGcm.NonceByteSizes.MaxSize - AesGcm.TagByteSizes.MaxSize;
            var encryptedBufferBytes = _pool.Rent(encryptedByteLength);
            var tagBytes = _pool.Rent(AesGcm.TagByteSizes.MaxSize);
            var nonceBytes = _pool.Rent(AesGcm.NonceByteSizes.MaxSize);

            var bytesRead = encryptedStream.Read(nonceBytes, 0, AesGcm.NonceByteSizes.MaxSize);
            if (bytesRead == 0) throw new InvalidDataException();

            bytesRead = encryptedStream.Read(tagBytes, 0, AesGcm.TagByteSizes.MaxSize);
            if(bytesRead == 0) throw new InvalidDataException();

            bytesRead = encryptedStream.Read(encryptedBufferBytes, 0, encryptedByteLength);
            if(bytesRead == 0) throw new InvalidDataException();

            // Slicing Version
            var nonce = nonceBytes
                .AsSpan()
                .Slice(0, AesGcm.NonceByteSizes.MaxSize);

            var tag = tagBytes
                .AsSpan()
                .Slice(0, AesGcm.TagByteSizes.MaxSize);

            var encryptedBytes = encryptedBufferBytes
                .AsSpan()
                .Slice(0, encryptedByteLength);

            var decryptedBytes = new byte[encryptedByteLength];
            aes.Decrypt(nonce, encryptedBytes, tag, decryptedBytes);

            _pool.Return(encryptedBufferBytes);
            _pool.Return(tagBytes);
            _pool.Return(nonceBytes);

            if (!leaveStreamOpen) { encryptedStream.Close(); }

            return RecyclableManager.GetStream(nameof(RecyclableAesGcmEncryptionProvider), decryptedBytes);
        }

        public MemoryStream DecryptToStream(ReadOnlyMemory<byte> encryptedData)
        {
            Guard.AgainstEmpty(encryptedData, nameof(encryptedData));

            return RecyclableManager.GetStream(nameof(RecyclableAesGcmEncryptionProvider), Decrypt(encryptedData));
        }
    }
}

using System;
using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Azure.Cosmos;

namespace Atc.Cosmos.Sql.Api.Repository.Serialization
{
    /// <summary>
    /// Custom cosmos JSON serializer implementation for System.Text.Json.
    /// </summary>
    public class TextJsonCosmosSerializer : CosmosSerializer
    {
        private const int UnSeekableStreamInitialRentSize = 4096;

        private readonly JsonSerializerOptions jsonSerializerOptions;

        public TextJsonCosmosSerializer()
        {
            jsonSerializerOptions = new JsonSerializerOptions
            {
                IgnoreNullValues = true,
                WriteIndented = true,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            };

            jsonSerializerOptions.Converters.Add(new TimeSpanConverter());
            jsonSerializerOptions.Converters.Add(new JsonStringEnumConverter());
        }

        private static ReadOnlySpan<byte> Utf8Bom => new byte[] { 0xEF, 0xBB, 0xBF };

        [return: MaybeNull]
        public override T FromStream<T>(Stream stream)
        {
            if (stream is null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            using (stream)
            {
                if (stream.CanSeek && stream.Length == 0)
                {
                    return default;
                }

                if (typeof(Stream).IsAssignableFrom(typeof(T)))
                {
                    return (T)(object)stream;
                }

                return Deserialize<T>(stream, jsonSerializerOptions);
            }
        }

        public override Stream ToStream<T>(T input)
        {
            var streamPayload = new MemoryStream();

            using var utf8JsonWriter = new Utf8JsonWriter(
                streamPayload,
                new JsonWriterOptions
                {
                    Indented = jsonSerializerOptions.WriteIndented,
                });

            JsonSerializer.Serialize(utf8JsonWriter, input, jsonSerializerOptions);
            streamPayload.Position = 0;

            return streamPayload;
        }

        /// <summary>
        /// Throughput and allocations optimized deserialization.
        /// </summary>
        /// <typeparam name="T">The generic type.</typeparam>
        /// <param name="stream">The stream.</param>
        /// <param name="jsonSerializerOptions">The json serializer options.</param>
        /// <returns>
        /// A deserialize version of the generic type.
        /// </returns>
        /// <remarks>
        /// Based off JsonDocument.ReadToEnd https://github.com/dotnet/runtime/blob/master/src/libraries/System.Text.Json/src/System/Text/Json/Document/JsonDocument.Parse.cs#L577.
        /// </remarks>
        [SuppressMessage("Design", "MA0051:Method is too long", Justification = "OK")]
        [SuppressMessage("Major Code Smell", "S1854:Unused assignments should be removed", Justification = "OK - Array.Empty is needed.")]
        internal static T Deserialize<T>(Stream stream, JsonSerializerOptions jsonSerializerOptions)
        {
            if (stream is MemoryStream memoryStream && memoryStream.TryGetBuffer(out var buffer))
            {
                if (buffer.Count >= Utf8Bom.Length && Utf8Bom.SequenceEqual(buffer.AsSpan(0, Utf8Bom.Length)))
                {
                    // Skip 3 BOM bytes
                    return JsonSerializer.Deserialize<T>(buffer.AsSpan(Utf8Bom.Length), jsonSerializerOptions);
                }

                return JsonSerializer.Deserialize<T>(buffer, jsonSerializerOptions);
            }

            var written = 0;
            byte[] rented = Array.Empty<byte>();
            var utf8Bom = Utf8Bom;

            try
            {
                if (stream.CanSeek)
                {
                    // Ask for 1 more than the length to avoid resizing later,
                    // which is unnecessary in the common case where the stream length doesn't change.
                    var expectedLength = Math.Max(utf8Bom.Length, stream.Length - stream.Position) + 1;
                    rented = ArrayPool<byte>.Shared.Rent(checked((int)expectedLength));
                }
                else
                {
                    rented = ArrayPool<byte>.Shared.Rent(UnSeekableStreamInitialRentSize);
                }

                int lastRead;

                // Read up to 3 bytes to see if it's the UTF-8 BOM
                do
                {
                    // No need for checking for growth, the minimal rent sizes both guarantee it'll fit.
                    Debug.Assert(rented.Length >= utf8Bom.Length, "Failed");

                    lastRead = stream.Read(
                        rented,
                        written,
                        utf8Bom.Length - written);

                    written += lastRead;
                }
                while (lastRead > 0 && written < utf8Bom.Length);

                // If we have 3 bytes, and they're the BOM, reset the write position to 0.
                if (written == utf8Bom.Length && utf8Bom.SequenceEqual(rented.AsSpan(0, utf8Bom.Length)))
                {
                    written = 0;
                }

                do
                {
                    if (rented.Length == written)
                    {
                        byte[] toReturn = rented;
                        rented = ArrayPool<byte>.Shared.Rent(checked(toReturn.Length * 2));
                        Buffer.BlockCopy(toReturn, 0, rented, 0, toReturn.Length);

                        // Holds document content, clear it.
                        ArrayPool<byte>.Shared.Return(toReturn, clearArray: true);
                    }

                    lastRead = stream.Read(rented, written, rented.Length - written);
                    written += lastRead;
                }
                while (lastRead > 0);

                return JsonSerializer.Deserialize<T>(rented.AsSpan(0, written), jsonSerializerOptions);
            }
            finally
            {
                // Holds document content, clear it before returning it.
                rented.AsSpan(0, written).Clear();
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }
}
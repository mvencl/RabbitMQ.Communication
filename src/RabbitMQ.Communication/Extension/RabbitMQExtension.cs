using Microsoft.Toolkit.HighPerformance;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Communication.Contracts;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace RabbitMQ.Communication.Extension
{
    public static class RabbitMQExtension
    {
        public static string CleanRoutingKey(string routingKey)
        {
            routingKey = routingKey.Replace('/', '.');
            if (routingKey.StartsWith(".")) routingKey = routingKey.Substring(1);
            return routingKey.ToLower().Trim();
        }

        public static string GetCorrelationId()
        {
            return Guid.NewGuid().ToString();
        }

        public static string GetDefaultSubscriberRoutingKey
        {
            get
            {
                return System.Reflection.Assembly.GetEntryAssembly()?.GetName()?.Name + "." + Guid.NewGuid().ToString();
            }
        }

        public static string GetDefaultSubscriberExchangeName
        {
            get
            {
                return "amq.direct";
            }
        }

        internal static void CreateQueue(this IModel channel, string queueName, bool exclusive = false)
        {
            Dictionary<string, object> args = new Dictionary<string, object>
            {
                { "x-message-ttl", 60000 },
                { "x-expires", 1800000 }
            };

            channel.QueueDeclare(queueName, true, exclusive, true, args);
        }

        public static ReadOnlyMemory<byte> SerializeObject<T>(T request, bool compress = true) where T : IMessageContext
        {
            byte[] compressedBytes;

            if (compress)
            {
                using (var uncompressedStream = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(request))))
                {
                    using (var compressedStream = new MemoryStream())
                    {
                        using (DeflateStream compressorStream = new DeflateStream(compressedStream, CompressionLevel.Fastest, true))
                        {
                            uncompressedStream.CopyTo(compressorStream);
                        }
                        compressedBytes = compressedStream.ToArray();
                    }

                }

            }
            else
            {
                compressedBytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(request));
            }

            return compressedBytes;

        }

        public static T DeserializeObject<T>(ReadOnlyMemory<byte> response, bool isCompressed = true)
        {
            if (isCompressed)
            {
                using (var uncompressedStream = new MemoryStream())
                {
                    using (DeflateStream compressorStream = new DeflateStream(response.AsStream(), CompressionMode.Decompress, true))
                    {
                        compressorStream.CopyTo(uncompressedStream);
                    }
                    return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(uncompressedStream.ToArray()));
                }
            }
            else
            {
                return JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(response.Span.ToArray()));
            }
        }

    }
}

using HouseofCat.Extensions;
using System.ComponentModel.DataAnnotations;

namespace HouseofCat.RabbitMQ
{
    public class RoutingOptions
    {
        [Range(1, 2, ErrorMessage = Constants.RangeErrorMessage)]
        public byte DeliveryMode { get; set; } = 2;

        public bool Mandatory { get; set; }

        // Max Priority letter level is 255, however, the max-queue priority though is 10, so > 10 is treated as 10.
        [Range(0, 10, ErrorMessage = Constants.RangeErrorMessage)]
        public byte PriorityLevel { get; set; }

        public string MessageType { get; set; } = $"{Enums.ContentType.Json.Description()} {Enums.Charset.Utf8.Description()}";

        public static RoutingOptions CreateDefaultRoutingOptions(byte priority = 0)
        {
            return new RoutingOptions
            {
                DeliveryMode = 2,
                Mandatory = false,
                PriorityLevel = priority
            };
        }
    }
}

using System.Threading.Channels;

namespace HouseofCat.RabbitMQ;

/// <summary>
/// Global overrides for your consumers.
/// </summary>
public class GlobalConsumerOptions
{
    public bool? NoLocal { get; set; }
    public bool? Exclusive { get; set; }
    public ushort? BatchSize { get; set; } = 5;
    public bool? AutoAck { get; set; }

    public string ErrorSuffix { get; set; }
    public string AltSuffix { get; set; }

    public BoundedChannelFullMode? BehaviorWhenFull { get; set; } = BoundedChannelFullMode.Wait;

    public ConsumerPipelineOptions GlobalConsumerPipelineOptions { get; set; }
}

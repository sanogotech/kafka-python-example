using System;
using System.Threading.Tasks;
using Confluent.Kafka;

//https://thecloudblog.net/post/building-reliable-kafka-producers-and-consumers-in-net/
class Program
{
    public static async Task Main(string[] args)
    {
		Console.WriteLine("Start  Kafka Producer ..!");
        var config = new ProducerConfig { 
		
		BootstrapServers = "localhost:9092,localhost:9094,localhost:9093",
		
		
		// retry settings:
        // Receive acknowledgement from all sync replicas
		/*
        Acks = Acks.All,
        // Number of times to retry before giving up
        MessageSendMaxRetries = 3,
        // Duration to retry before next attempt
        RetryBackoffMs = 1000,
        // Set to true if you don't want to reorder messages on retry
        EnableIdempotence = true
		*/
		
		};

        // If serializers are not specified, default serializers from
        // `Confluent.Kafka.Serializers` will be automatically used where
        // available. Note: by default strings are encoded as UTF8.
        using (var p = new ProducerBuilder<Null, string>(config).Build())
        {
            try
            {
                var dr = await p.ProduceAsync("supervisionmonitoring", new Message<Null, string> { Value="test DOT NET" });
                Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
            }
            catch (ProduceException<Null, string> e)
            {
                Console.WriteLine($"Delivery failed: {e.Error.Reason}");
            }
        }
    }
}

namespace RabbitMqPoc;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

internal class Program
{

    private static void Main(string[] args)
    {
        var host = "localhost";

        // Alpha Subscriber 1
        IRabbitMqConnectionManager alphaSubscriberConnectionManager1 = new RabbitMqConnectionManager(host);
        IRabbitMqPubSubClient<AlphaMessage> alphaSubscriber1 = new RabbitMqPubSubClient<AlphaMessage>(alphaSubscriberConnectionManager1);
        alphaSubscriber1.Subscribe(OnAlphaMessage);

        // Alpha Subscriber 2
        IRabbitMqConnectionManager alphaSubscriberConnectionManager2 = new RabbitMqConnectionManager(host);
        IRabbitMqPubSubClient<AlphaMessage> alphaSubscriber2 = new RabbitMqPubSubClient<AlphaMessage>(alphaSubscriberConnectionManager2);
        alphaSubscriber2.Subscribe(OnAlphaMessage);

        // Alpha Publisher 1
        IRabbitMqConnectionManager alphaPublisherConnectionManager1 = new RabbitMqConnectionManager(host);
        IRabbitMqPubSubClient<AlphaMessage> alphaPublisher1 = new RabbitMqPubSubClient<AlphaMessage>(alphaPublisherConnectionManager1);
        AlphaMessage alphaMessage = new AlphaMessage { Id = "1", Payload = "A message for Alphas." };
        alphaPublisher1.Publish(alphaMessage);

        // Beta Subscriber 1
        IRabbitMqConnectionManager betaSubscriberConnectionManager1 = new RabbitMqConnectionManager(host);
        IRabbitMqPubSubClient<BetaMessage> betaSubscriber1 = new RabbitMqPubSubClient<BetaMessage>(betaSubscriberConnectionManager1);
        betaSubscriber1.Subscribe(OnBetaMessage);

        // Beta Publisher 1
        IRabbitMqConnectionManager betaPublisherConnectionManager1 = new RabbitMqConnectionManager(host);
        IRabbitMqPubSubClient<BetaMessage> betaPublisher1 = new RabbitMqPubSubClient<BetaMessage>(betaPublisherConnectionManager1);
        BetaMessage betaMessage = new BetaMessage { Id = "2", Payload = "A message for Betas." };
        betaPublisher1.Publish(betaMessage);

        Console.ReadLine();
    }

    static void DoSomeWork(object sender, BasicDeliverEventArgs ea)
    {
        var body = ea.Body.ToArray();
        var message = Encoding.UTF8.GetString(body);
        var routingKey = ea.RoutingKey;

        var eventingBasicConsumer = sender as EventingBasicConsumer;
        var currentQueue = eventingBasicConsumer?.Model.CurrentQueue;

        Console.WriteLine($" [x] Received '{currentQueue}':'{routingKey}':'{message}'");
    }

    static void OnAlphaMessage(object sender, BasicDeliverEventArgs ea)
    {
        // This would call Alpha type message processing, but calling common DoSomeWork for ease.
        DoSomeWork(sender, ea);
    }

    static void OnBetaMessage(object sender, BasicDeliverEventArgs ea)
    {
        // This would call Beta type message processing, but calling common DoSomeWork for ease.
        DoSomeWork(sender, ea);
    }
}

public interface IMessage<Tid, TPayload>
{
    Tid Id { get; set; }
    TPayload Payload { get; set; }   
}

public interface ITypedMessage<TPayload> : IMessage<string, TPayload>
{ 
}

//public abstract class BaseTypedMessage<TPayload> : ITypedMessage<TPayload>
//{
//    public string Id { get; set; }
//    public TPayload Payload { get; set; }
//}

public class AlphaMessage : ITypedMessage<string>
{
    public string Id { get; set; }
    public string Payload { get; set; }
}

public class BetaMessage : ITypedMessage<string>
{
    public string Id { get; set; }
    public string Payload { get; set; }
}

public class GammaMessage : ITypedMessage<string>
{
    public string Id { get; set; }
    public string Payload { get; set; }
}

public interface IRabbitMqConnectionManager
{
    bool IsConnected { get; }
    IModel? Channel { get; }

    void Connect();
    void Disconnect();
}

public class RabbitMqConnectionManager : IRabbitMqConnectionManager
{
    private ConnectionFactory? factory;
    private IConnection? connection;
    private IModel? channel;
    private readonly string host;

    public bool IsConnected =>
        connection != null && connection.IsOpen
        && channel != null && channel.IsOpen;

    // TODO: move the channel out of connections, so multiple channels can share the same tcp connection
    public IModel? Channel => channel;

    public RabbitMqConnectionManager(string host)
    {
        this.host = host;
    }

    public void Connect()
    {
        try
        {
            factory = new ConnectionFactory { HostName = host };
            connection = factory.CreateConnection();
            channel = connection.CreateModel();
        }
        catch
        {
            // TODO: 
        }
    }

    public void Disconnect()
    {
        try
        {
            if (IsConnected)
            {
                // closes the connection and channel
                connection!.Close();
            }
        }
        catch
        {
            // TODO:
        }
    }
}

public interface IRabbitMqPubSubClient<TMessage>
{
    void Publish(TMessage message);
    void Subscribe(EventHandler<BasicDeliverEventArgs> received);
}

public class RabbitMqPubSubClient<T> : IRabbitMqPubSubClient<T> where T : ITypedMessage<string>
{
    private const string Exchange = "direct_rabbitmq_poc";
    private readonly string routingKey;
    private readonly IRabbitMqConnectionManager rabbitMqConnectionManager;

    public RabbitMqPubSubClient(IRabbitMqConnectionManager rabbitMqConnectionManager)
    {
        this.rabbitMqConnectionManager = rabbitMqConnectionManager;
        this.routingKey = typeof(T).Name;
    }

    public void Publish(T message)
    {
        if (!rabbitMqConnectionManager.IsConnected)
        {
            rabbitMqConnectionManager.Connect();
        }

        rabbitMqConnectionManager.Channel!.ExchangeDeclare(exchange: Exchange, type: ExchangeType.Direct);
        
        var body = Encoding.UTF8.GetBytes(message.Payload);
        rabbitMqConnectionManager.Channel!.BasicPublish(exchange: Exchange,
                             routingKey: routingKey,
                             basicProperties: null,
                             body: body);

        Console.WriteLine($" [x] Sent '{message.Payload}'");
    }

    public void Subscribe(EventHandler<BasicDeliverEventArgs> received)
    {
        if (!rabbitMqConnectionManager.IsConnected)
        {
            rabbitMqConnectionManager.Connect();
        }
        
        rabbitMqConnectionManager.Channel!.ExchangeDeclare(exchange: Exchange, type: ExchangeType.Direct);

        var queueName = rabbitMqConnectionManager.Channel!.QueueDeclare().QueueName;

        rabbitMqConnectionManager.Channel!.QueueBind(queue: queueName,
                      exchange: Exchange,
                      routingKey: routingKey);

        var consumer = new EventingBasicConsumer(rabbitMqConnectionManager.Channel!);

        consumer.Received += received;

        rabbitMqConnectionManager.Channel!.BasicConsume(queue: queueName,
                             autoAck: true,
                             consumer: consumer);
    }
}

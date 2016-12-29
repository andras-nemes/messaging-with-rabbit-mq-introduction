using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.MessagePatterns;
using RabbitMQ.Client.Events;
using System.Threading;
using System.Diagnostics;

namespace RabbitMqNetTests
{
	class Program
	{
		static void Main(string[] args)
		{
			SetUpDirectExchange();
			Console.ReadKey();
		}
		private static void SetUpHeadersExchange()
		{
			ConnectionFactory connectionFactory = new ConnectionFactory();

			connectionFactory.Port = 5672;
			connectionFactory.HostName = "localhost";
			connectionFactory.UserName = "accountant";
			connectionFactory.Password = "accountant";
			connectionFactory.VirtualHost = "accounting";

			IConnection connection = connectionFactory.CreateConnection();

			IModel channel = connection.CreateModel();
			

			channel.ExchangeDeclare("company.exchange.headers", ExchangeType.Headers, true, false, null);
			channel.QueueDeclare("company.queue.headers", true, false, false, null);
			Dictionary<string, object> headerOptionsWithAll = new Dictionary<string, object>();
			headerOptionsWithAll.Add("x-match", "all");
			headerOptionsWithAll.Add("category", "animal");
			headerOptionsWithAll.Add("type", "mammal");

			channel.QueueBind("company.queue.headers", "company.exchange.headers", "", headerOptionsWithAll);

			Dictionary<string, object> headerOptionsWithAny = new Dictionary<string, object>();
			headerOptionsWithAny.Add("x-match", "any");
			headerOptionsWithAny.Add("category", "plant");
			headerOptionsWithAny.Add("type", "tree");

			channel.QueueBind("company.queue.headers", "company.exchange.headers", "", headerOptionsWithAny);

			IBasicProperties properties = channel.CreateBasicProperties();
			Dictionary<string, object> messageHeaders = new Dictionary<string, object>();
			messageHeaders.Add("category", "animal");
			messageHeaders.Add("type", "insect");
			properties.Headers = messageHeaders;
			PublicationAddress address = new PublicationAddress(ExchangeType.Headers, "company.exchange.headers", "");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of insects"));

			properties = channel.CreateBasicProperties();
			messageHeaders = new Dictionary<string, object>();
			messageHeaders.Add("category", "animal");
			messageHeaders.Add("type", "mammal");
			messageHeaders.Add("mood", "awesome");
			properties.Headers = messageHeaders;
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of awesome mammals"));

			properties = channel.CreateBasicProperties();
			messageHeaders = new Dictionary<string, object>();
			messageHeaders.Add("category", "animal");
			messageHeaders.Add("type", "mammal");
			properties.Headers = messageHeaders;
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of mammals"));

			properties = channel.CreateBasicProperties();
			messageHeaders = new Dictionary<string, object>();
			messageHeaders.Add("category", "animal");
			properties.Headers = messageHeaders;
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of animals"));

			properties = channel.CreateBasicProperties();
			messageHeaders = new Dictionary<string, object>();
			messageHeaders.Add("category", "fungi");
			messageHeaders.Add("type", "champignon");
			properties.Headers = messageHeaders;
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of fungi"));

			properties = channel.CreateBasicProperties();
			messageHeaders = new Dictionary<string, object>();
			messageHeaders.Add("category", "plant");
			properties.Headers = messageHeaders;
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of plants"));

			properties = channel.CreateBasicProperties();
			messageHeaders = new Dictionary<string, object>();
			messageHeaders.Add("category", "plant");
			messageHeaders.Add("type", "tree");
			properties.Headers = messageHeaders;
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of trees"));

			properties = channel.CreateBasicProperties();
			messageHeaders = new Dictionary<string, object>();
			messageHeaders.Add("mood", "sad");
			messageHeaders.Add("type", "tree");
			properties.Headers = messageHeaders;
			
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello from the world of sad trees"));

			channel.Close();
			connection.Close();
		}


		private static void SetUpTopicsExchange()
		{
			ConnectionFactory connectionFactory = new ConnectionFactory();

			connectionFactory.Port = 5672;
			connectionFactory.HostName = "localhost";
			connectionFactory.UserName = "accountant";
			connectionFactory.Password = "accountant";
			connectionFactory.VirtualHost = "accounting";

			IConnection connection = connectionFactory.CreateConnection();
			IModel channel = connection.CreateModel();

			channel.ExchangeDeclare("company.exchange.topic", ExchangeType.Topic, true, false, null);
			channel.QueueDeclare("company.queue.topic", true, false, false, null);
			channel.QueueBind("company.queue.topic", "company.exchange.topic", "*.world");
			channel.QueueBind("company.queue.topic", "company.exchange.topic", "world.#");

			IBasicProperties properties = channel.CreateBasicProperties();
			properties.Persistent = true;
			properties.ContentType = "text/plain";
			PublicationAddress address = new PublicationAddress(ExchangeType.Topic, "company.exchange.topic", "news of the world");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("This is some random news from the world"));

			address = new PublicationAddress(ExchangeType.Topic, "company.exchange.topic", "news.of.the.world");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("trololo"));

			address = new PublicationAddress(ExchangeType.Topic, "company.exchange.topic", "the world is crumbling");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("whatever"));

			address = new PublicationAddress(ExchangeType.Topic, "company.exchange.topic", "the.world.is.crumbling");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("Hello"));

			address = new PublicationAddress(ExchangeType.Topic, "company.exchange.topic", "world.news.and.more");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("It's Friday night"));

			address = new PublicationAddress(ExchangeType.Topic, "company.exchange.topic", "world news and more");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("No more tears"));

			address = new PublicationAddress(ExchangeType.Topic, "company.exchange.topic", "beautiful.world");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("The world is beautiful"));

			channel.Close();
			connection.Close();
		}

		private static void SetUpDirectExchangeWithRoutingKey()
		{
			ConnectionFactory connectionFactory = new ConnectionFactory();

			connectionFactory.Port = 5672;
			connectionFactory.HostName = "localhost";
			connectionFactory.UserName = "accountant";
			connectionFactory.Password = "accountant";
			connectionFactory.VirtualHost = "accounting";

			IConnection connection = connectionFactory.CreateConnection();
			IModel channel = connection.CreateModel();

			channel.ExchangeDeclare("company.exchange.routing", ExchangeType.Direct, true, false, null);
			channel.QueueDeclare("company.exchange.queue", true, false, false, null);
			channel.QueueBind("company.exchange.queue", "company.exchange.routing", "asia");
			channel.QueueBind("company.exchange.queue", "company.exchange.routing", "americas");
			channel.QueueBind("company.exchange.queue", "company.exchange.routing", "europe");

			IBasicProperties properties = channel.CreateBasicProperties();
			properties.Persistent = true;
			properties.ContentType = "text/plain";
			PublicationAddress address = new PublicationAddress(ExchangeType.Direct, "company.exchange.routing", "asia");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("The latest news from Asia!"));

			address = new PublicationAddress(ExchangeType.Direct, "company.exchange.routing", "europe");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("The latest news from Europe!"));

			address = new PublicationAddress(ExchangeType.Direct, "company.exchange.routing", "americas");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("The latest news from the Americas!"));

			address = new PublicationAddress(ExchangeType.Direct, "company.exchange.routing", "africa");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("The latest news from Africa!"));

			address = new PublicationAddress(ExchangeType.Direct, "company.exchange.routing", "australia");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("The latest news from Australia!"));

			channel.Close();
			connection.Close();
		}

		private static void RunScatterGatherQueue()
		{
			ConnectionFactory connectionFactory = new ConnectionFactory();

			connectionFactory.Port = 5672;
			connectionFactory.HostName = "localhost";
			connectionFactory.UserName = "accountant";
			connectionFactory.Password = "accountant";
			connectionFactory.VirtualHost = "accounting";

			IConnection connection = connectionFactory.CreateConnection();
			IModel channel = connection.CreateModel();

			channel.QueueDeclare("mycompany.queues.scattergather.a", true, false, false, null);
			channel.QueueDeclare("mycompany.queues.scattergather.b", true, false, false, null);
			channel.QueueDeclare("mycompany.queues.scattergather.c", true, false, false, null);
			channel.ExchangeDeclare("mycompany.exchanges.scattergather", ExchangeType.Fanout, true, false, null);
			channel.QueueBind("mycompany.queues.scattergather.a", "mycompany.exchanges.scattergather", "");
			channel.QueueBind("mycompany.queues.scattergather.b", "mycompany.exchanges.scattergather", "");
			channel.QueueBind("mycompany.queues.scattergather.c", "mycompany.exchanges.scattergather", "");
			SendScatterGatherMessages(connection, channel, 3);			
		}

		private static void SendScatterGatherMessages(IConnection connection, IModel channel, int minResponses)
		{
			List<string> responses = new List<string>();
			string rpcResponseQueue = channel.QueueDeclare().QueueName;
			string correlationId = Guid.NewGuid().ToString();

			IBasicProperties basicProperties = channel.CreateBasicProperties();
			basicProperties.ReplyTo = rpcResponseQueue;
			basicProperties.CorrelationId = correlationId;
			Console.WriteLine("Enter your message and press Enter.");

			string message = Console.ReadLine();
			byte[] messageBytes = Encoding.UTF8.GetBytes(message);
			channel.BasicPublish("mycompany.exchanges.scattergather", "", basicProperties, messageBytes);			

			EventingBasicConsumer scatterGatherEventingBasicConsumer = new EventingBasicConsumer(channel);
			scatterGatherEventingBasicConsumer.Received += (sender, basicDeliveryEventArgs) =>
			{
				IBasicProperties props = basicDeliveryEventArgs.BasicProperties;
				channel.BasicAck(basicDeliveryEventArgs.DeliveryTag, false);
				if (props != null
					&& props.CorrelationId == correlationId)
				{
					string response = Encoding.UTF8.GetString(basicDeliveryEventArgs.Body);
					Console.WriteLine("Response: {0}", response);
					responses.Add(response);
					if (responses.Count >= minResponses)
					{
						Console.WriteLine(string.Concat("Responses received from consumers: ", string.Join(Environment.NewLine, responses)));
						channel.Close();
						connection.Close();
					}
				}								
			};
			channel.BasicConsume(rpcResponseQueue, false, scatterGatherEventingBasicConsumer);			
		}

		private static void RunRpcQueue()
		{
			ConnectionFactory connectionFactory = new ConnectionFactory();

			connectionFactory.Port = 5672;
			connectionFactory.HostName = "localhost";
			connectionFactory.UserName = "accountant";
			connectionFactory.Password = "accountant";
			connectionFactory.VirtualHost = "accounting";

			IConnection connection = connectionFactory.CreateConnection();
			IModel channel = connection.CreateModel();

			channel.QueueDeclare("mycompany.queues.rpc", true, false, false, null);
			SendRpcMessagesBackAndForth(channel);			
		}

		private static void SendRpcMessagesBackAndForth(IModel channel)
		{
			string rpcResponseQueue = channel.QueueDeclare().QueueName;

			string correlationId = Guid.NewGuid().ToString();
			string responseFromConsumer = null;

			IBasicProperties basicProperties = channel.CreateBasicProperties();
			basicProperties.ReplyTo = rpcResponseQueue;
			basicProperties.CorrelationId = correlationId;
			Console.WriteLine("Enter your message and press Enter.");
			string message = Console.ReadLine();
			byte[] messageBytes = Encoding.UTF8.GetBytes(message);
			channel.BasicPublish("", "mycompany.queues.rpc", basicProperties, messageBytes);

			EventingBasicConsumer rpcEventingBasicConsumer = new EventingBasicConsumer(channel);
			rpcEventingBasicConsumer.Received += (sender, basicDeliveryEventArgs) =>
			{
				IBasicProperties props = basicDeliveryEventArgs.BasicProperties;
				if (props != null
					&& props.CorrelationId == correlationId)
				{
					string response = Encoding.UTF8.GetString(basicDeliveryEventArgs.Body);
					responseFromConsumer = response;
				}
				channel.BasicAck(basicDeliveryEventArgs.DeliveryTag, false);
				Console.WriteLine("Response: {0}", responseFromConsumer);
				Console.WriteLine("Enter your message and press Enter.");
				message = Console.ReadLine();
				messageBytes = Encoding.UTF8.GetBytes(message);
				channel.BasicPublish("", "mycompany.queues.rpc", basicProperties, messageBytes);
			};
			channel.BasicConsume(rpcResponseQueue, false, rpcEventingBasicConsumer);
		}

		private static void SetUpFanoutExchange()
		{
			ConnectionFactory connectionFactory = new ConnectionFactory();

			connectionFactory.Port = 5672;
			connectionFactory.HostName = "localhost";
			connectionFactory.UserName = "accountant";
			connectionFactory.Password = "accountant";
			connectionFactory.VirtualHost = "accounting";

			IConnection connection = connectionFactory.CreateConnection();
			IModel channel = connection.CreateModel();

			channel.ExchangeDeclare("mycompany.fanout.exchange", ExchangeType.Fanout, true, false, null);
			channel.QueueDeclare("mycompany.queues.accounting", true, false, false, null);
			channel.QueueDeclare("mycompany.queues.management", true, false, false, null);
			channel.QueueBind("mycompany.queues.accounting", "mycompany.fanout.exchange", "");
			channel.QueueBind("mycompany.queues.management", "mycompany.fanout.exchange", "");

			IBasicProperties properties = channel.CreateBasicProperties();
			properties.Persistent = true;
			properties.ContentType = "text/plain";
			PublicationAddress address = new PublicationAddress(ExchangeType.Fanout, "mycompany.fanout.exchange", "");
			channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("A new huge order has just come in worth $1M!!!!!"));

			channel.Close();
			connection.Close();
			Console.WriteLine(string.Concat("Channel is closed: ", channel.IsClosed));

			Console.WriteLine("Main done...");
		}

		private static void SetUpDirectExchange()
		{
			ConnectionFactory connectionFactory = new ConnectionFactory();

			connectionFactory.AutomaticRecoveryEnabled = true;
			connectionFactory.TopologyRecoveryEnabled = true;
			connectionFactory.NetworkRecoveryInterval = TimeSpan.FromSeconds(30);

			connectionFactory.Port = 5672;
			connectionFactory.HostName = "localhost";
			connectionFactory.UserName = "accountant";
			connectionFactory.Password = "accountant";
			connectionFactory.VirtualHost = "accounting";

			IConnection connection = connectionFactory.CreateConnection();
			//connectionFactory.Uri = "amqp://accountant:accountant@localhost:5672/accounting";
			IModel channel = connection.CreateModel();

			channel.ExchangeDeclare("no.queue.exchange", ExchangeType.Direct, true, false, null);			
			//channel.QueueDeclare("my.first.queue", true, false, false, null);			
			//channel.QueueBind("my.first.queue", "my.first.exchange", "");
			//channel.ConfirmSelect();
			//channel.BasicAcks += Channel_BasicAcks;
			//channel.BasicNacks += Channel_BasicNacks;

			IBasicProperties properties = channel.CreateBasicProperties();
			channel.BasicReturn += Channel_BasicReturn;
						
			//PublicationAddress address = new PublicationAddress(ExchangeType.Direct, "my.first.exchange", "");		
			//channel.BasicPublish(address, properties, Encoding.UTF8.GetBytes("This is a message from the RabbitMq .NET driver"));		
			channel.BasicPublish("no.queue.exchange", "", true, properties, Encoding.UTF8.GetBytes("This is a message from the RabbitMq .NET driver"));

			channel.Close();
			connection.Close();

			Console.WriteLine("Main done...");
		}

		private static void Channel_BasicReturn(object sender, BasicReturnEventArgs e)
		{
			Debug.WriteLine(string.Concat("Queue is missing for the message: ", Encoding.UTF8.GetString(e.Body)));
			Debug.WriteLine(string.Concat("Reply code and text: ", e.ReplyCode, " ", e.ReplyText));
		}

		private static void Channel_BasicNacks(object sender, BasicNackEventArgs e)
		{
			Console.WriteLine(string.Concat("Message broker could not acknowledge messaage with tag: ", e.DeliveryTag));
		}

		private static void Channel_BasicAcks(object sender, BasicAckEventArgs e)
		{
			Console.WriteLine(string.Concat("Message broker has acknowledged messaage with tag: ", e.DeliveryTag));
		}
	}
}

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMq.OneWayMessage.Receiver
{
	class Program
	{
		private static IModel channelForEventing;

		static void Main(string[] args)
		{
			ReceiveSingleOneWayMessage();
		}

		private static void ReceiveSingleOneWayMessage()
		{
			ConnectionFactory connectionFactory = new ConnectionFactory();

			connectionFactory.Port = 5672;
			connectionFactory.HostName = "localhost";
			connectionFactory.UserName = "accountant";
			connectionFactory.Password = "accountant";
			connectionFactory.VirtualHost = "accounting";

			IConnection connection = connectionFactory.CreateConnection();
			IModel channel = connection.CreateModel();
			channel.BasicQos(0, 1, false);
			DefaultBasicConsumer basicConsumer = new OneWayMessageReceiver(channel);			
			channel.BasicConsume("my.first.queue", false, basicConsumer);			
		}

		private static void ReceiveMessagesWithEvents()
		{
			ConnectionFactory connectionFactory = new ConnectionFactory();

			connectionFactory.Port = 5672;
			connectionFactory.HostName = "localhost";
			connectionFactory.UserName = "accountant";
			connectionFactory.Password = "accountant";
			connectionFactory.VirtualHost = "accounting";

			IConnection connection = connectionFactory.CreateConnection();
			IModel channel = connection.CreateModel();
			channel.BasicQos(0, 1, false);
			EventingBasicConsumer eventingBasicConsumer = new EventingBasicConsumer(channel);

			eventingBasicConsumer.Received += (sender, basicDeliveryEventArgs) =>
			{
				IBasicProperties basicProperties = basicDeliveryEventArgs.BasicProperties;
				Console.WriteLine("Message received by the event based consumer. Check the debug window for details.");
				Debug.WriteLine(string.Concat("Message received from the exchange ", basicDeliveryEventArgs.Exchange));
				Debug.WriteLine(string.Concat("Routing key: ", basicDeliveryEventArgs.RoutingKey));
				Debug.WriteLine(string.Concat("Message: ", Encoding.UTF8.GetString(basicDeliveryEventArgs.Body)));
				StringBuilder headersBuilder = new StringBuilder();
				headersBuilder.Append("Headers: ").Append(Environment.NewLine);
				foreach (var kvp in basicProperties.Headers)
				{
					headersBuilder.Append(kvp.Key).Append(": ").Append(Encoding.UTF8.GetString(kvp.Value as byte[])).Append(Environment.NewLine);
				}
				Debug.WriteLine(headersBuilder.ToString());
				channel.BasicAck(basicDeliveryEventArgs.DeliveryTag, false);				
			};	
			
			channel.BasicConsume("company.queue.headers", false, eventingBasicConsumer);
		}

		private static void EventingBasicConsumer_Received(object sender, BasicDeliverEventArgs e)
		{
			IBasicProperties basicProperties = e.BasicProperties;
			Console.WriteLine("Message received by the event based consumer. Check the debug window for details.");
			Debug.WriteLine(string.Concat("Message received from the exchange ", e.Exchange));
			Debug.WriteLine(string.Concat("Content type: ", basicProperties.ContentType));
			Debug.WriteLine(string.Concat("Consumer tag: ", e.ConsumerTag));
			Debug.WriteLine(string.Concat("Delivery tag: ", e.DeliveryTag));
			Debug.WriteLine(string.Concat("Message: ", Encoding.UTF8.GetString(e.Body)));
			channelForEventing.BasicAck(e.DeliveryTag, false);
		}
	}
}

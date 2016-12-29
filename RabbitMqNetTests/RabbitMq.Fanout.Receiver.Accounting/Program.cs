using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMq.Fanout.Receiver.Accounting
{
	class Program
	{
		static void Main(string[] args)
		{
			ReceiveFanoutMessages();
		}

		private static void ReceiveFanoutMessages()
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

				Debug.WriteLine(string.Concat("Message received from the exchange ", basicDeliveryEventArgs.Exchange));
				Debug.WriteLine(string.Concat("Content type: ", basicProperties.ContentType));
				Debug.WriteLine(string.Concat("Consumer tag: ", basicDeliveryEventArgs.ConsumerTag));
				Debug.WriteLine(string.Concat("Delivery tag: ", basicDeliveryEventArgs.DeliveryTag));
				string message = Encoding.UTF8.GetString(basicDeliveryEventArgs.Body);
				Debug.WriteLine(string.Concat("Message: ", Encoding.UTF8.GetString(basicDeliveryEventArgs.Body)));
				Console.WriteLine(string.Concat("Message received by the accounting consumer: ", message));
				channel.BasicAck(basicDeliveryEventArgs.DeliveryTag, false);
			};

			channel.BasicConsume("mycompany.queues.accounting", false, eventingBasicConsumer);
		}
	}
}

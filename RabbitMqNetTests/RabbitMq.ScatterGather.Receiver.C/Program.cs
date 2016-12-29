﻿using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMq.ScatterGather.Receiver.C
{
	class Program
	{
		static void Main(string[] args)
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
			string consumerId = "C";
			Console.WriteLine(string.Concat("Consumer ", consumerId, " up and running, waiting for the publisher to start the bidding process."));
			eventingBasicConsumer.Received += (sender, basicDeliveryEventArgs) =>
			{
				string message = Encoding.UTF8.GetString(basicDeliveryEventArgs.Body);
				channel.BasicAck(basicDeliveryEventArgs.DeliveryTag, false);
				Console.WriteLine("Message: {0} {1}", message, " Enter your response: ");
				string response = string.Concat("Consumer ID: ", consumerId, ", bid: ", Console.ReadLine());
				IBasicProperties replyBasicProperties = channel.CreateBasicProperties();
				replyBasicProperties.CorrelationId = basicDeliveryEventArgs.BasicProperties.CorrelationId;
				byte[] responseBytes = Encoding.UTF8.GetBytes(response);
				channel.BasicPublish("", basicDeliveryEventArgs.BasicProperties.ReplyTo, replyBasicProperties, responseBytes);
				channel.Close();
				connection.Close();
			};
			channel.BasicConsume("mycompany.queues.scattergather.c", false, eventingBasicConsumer);
		}
	}
}

'use strict'

const fp = require('fastify-plugin')

module.exports = fp(async function (fastify, opts) {
  fastify.decorate('sendMessage', function (message) {
    const body = message.toString()
    
    // Check if we have a connection string for Azure Service Bus
    if (process.env.SERVICEBUS_CONNECTION_STRING) {
      const { ServiceBusClient } = require("@azure/service-bus");
      
      const queueName = process.env.SERVICEBUS_QUEUE_NAME || process.env.ORDER_QUEUE_NAME || "orders";
      
      console.log(`Sending message ${body} to ${queueName} using Azure Service Bus connection string`);
      
      async function sendMessage() {
        // Create a Service Bus client using the connection string
        const sbClient = new ServiceBusClient(process.env.SERVICEBUS_CONNECTION_STRING);
        
        // Create a sender for the queue
        const sender = sbClient.createSender(queueName);
        
        try {
          // Send the message
          await sender.sendMessages({ body: body });
          console.log(`Message sent successfully to queue ${queueName}`);
        } catch (error) {
          console.error("Error sending message to Azure Service Bus:", error);
        } finally {
          // Close the sender and client
          await sender.close();
          await sbClient.close();
        }
      }
      
      // Call the async function and catch any errors
      sendMessage().catch(error => {
        console.error("Failed to send message:", error);
      });
    } 
    // Fallback to workload identity auth if configured
    else if (process.env.USE_WORKLOAD_IDENTITY_AUTH === 'true') {
      const { ServiceBusClient } = require("@azure/service-bus");
      const { DefaultAzureCredential } = require("@azure/identity");

      const fullyQualifiedNamespace = process.env.ORDER_QUEUE_HOSTNAME || process.env.AZURE_SERVICEBUS_FULLYQUALIFIEDNAMESPACE;

      console.log(`Sending message ${body} to ${process.env.ORDER_QUEUE_NAME} on ${fullyQualifiedNamespace} using Microsoft Entra ID Workload Identity credentials`);
      
      if (!fullyQualifiedNamespace) {
        console.log('No hostname set for message queue. Exiting.');
        return;
      }
      
      const queueName = process.env.ORDER_QUEUE_NAME || "orders";

      const credential = new DefaultAzureCredential();

      async function sendMessage() {
        const sbClient = new ServiceBusClient(fullyQualifiedNamespace, credential);
        const sender = sbClient.createSender(queueName);

        try {
          await sender.sendMessages({ body: body });
          console.log(`Message sent successfully to queue ${queueName}`);
        } catch (error) {
          console.error("Error sending message:", error);
        } finally {
          await sender.close();
          await sbClient.close();
        }
      }
      
      sendMessage().catch(console.error);
    } 
    // Legacy support for RabbitMQ (optional - can be removed if not needed)
    else if (process.env.ORDER_QUEUE_USERNAME && process.env.ORDER_QUEUE_PASSWORD) {
      console.log(`Sending message ${body} to ${process.env.ORDER_QUEUE_NAME} on ${process.env.ORDER_QUEUE_HOSTNAME} using local auth credentials`);
      
      const rhea = require('rhea');
      const container = rhea.create_container();
      var amqp_message = container.message;

      const connectOptions = {
        hostname: process.env.ORDER_QUEUE_HOSTNAME,
        host: process.env.ORDER_QUEUE_HOSTNAME,
        port: process.env.ORDER_QUEUE_PORT,
        username: process.env.ORDER_QUEUE_USERNAME,
        password: process.env.ORDER_QUEUE_PASSWORD,
        reconnect_limit: process.env.ORDER_QUEUE_RECONNECT_LIMIT || 0
      };
      
      if (process.env.ORDER_QUEUE_TRANSPORT !== undefined) {
        connectOptions.transport = process.env.ORDER_QUEUE_TRANSPORT;
      }
      
      const connection = container.connect(connectOptions);
      
      container.once('sendable', function (context) {
        const sender = context.sender;
        sender.send({
          body: amqp_message.data_section(Buffer.from(body,'utf8'))
        });
        sender.close();
        connection.close();
      });

      connection.open_sender(process.env.ORDER_QUEUE_NAME);
    } else {
      console.log('No credentials set for message queue. Please configure SERVICEBUS_CONNECTION_STRING or other authentication method. Exiting.')
      return;
    }
  })
})
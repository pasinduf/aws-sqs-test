using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace SQS_Demo
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("start");

            //create new queue
            //SQSServices.CreateQueueAsync();

            //send message to queue
           // await SQSServices.AddMessageQueueAsync();

            //read messages from queue
            await SQSServices.ReadFromQueueAsync();

            Console.WriteLine("end");
            Console.ReadLine();
        }
    }


    public static class SQSServices
    {
        private const string queueUrl = "https://sqs.us-east-2.amazonaws.com/601172262855/demo";
        private static IAmazonSQS sqs = new AmazonSQSClient(RegionEndpoint.USEast2);
        private static object message;

        public static void CreateQueueAsync()
        {
            IAmazonSQS sqs = new AmazonSQSClient(RegionEndpoint.USEast2);
            try
            {
                var sqsRequest = new CreateQueueRequest { QueueName = "test" };
                var response = sqs.CreateQueueAsync(sqsRequest).Result;
                Console.WriteLine($"created : {response.QueueUrl}");
            }
            catch(Exception e){}
        }

        public static async Task AddMessageQueueAsync()
        {
            var requestBody = new QueueMessage { Id = 4, Name = "name 4", Age = 40 };
            try
            {
                string message = JsonConvert.SerializeObject(requestBody);
                var sendRequest = new SendMessageRequest(queueUrl, message);
                // Post message or payload to queue  
                var sendResult = await sqs.SendMessageAsync(sendRequest);
                Console.WriteLine($"reponse status: {sendResult.HttpStatusCode}");
            }
            catch(Exception e) { }
        }

        public static async Task ReadFromQueueAsync()
        {
            try
            {
                var request = new ReceiveMessageRequest
                {
                    QueueUrl = queueUrl,
                    WaitTimeSeconds = 10
                };
                //CheckIs there any new message available to process  
                var result = sqs.ReceiveMessageAsync(request).Result;
                var messages = result.Messages;
                messages.ForEach(async message =>
                {
                    Console.WriteLine($"Message Id: {message.MessageId}");
                    Console.WriteLine($"Body: {message.Body}");
                    await SQSServices.DeleteFromQueueAsync(message.ReceiptHandle);
                });
            }
            catch (Exception e) { }
        }

        //Delete from sqs
        public static async Task DeleteFromQueueAsync(string receiptHandle)
        {
            try
            {
                var deleteRequest = new DeleteMessageRequest { QueueUrl = queueUrl, ReceiptHandle = receiptHandle };
                //Deletes the specified message from the specified queue  
                var deleteResult = await sqs.DeleteMessageAsync(deleteRequest);
                Console.WriteLine($"response status: {deleteResult.HttpStatusCode}");
            }
            catch (Exception e) { }
        }


        public class QueueMessage
        {
            public int Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }
        }
    }
}

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading.Tasks;

namespace MessageProcessorMicroserviceApp
{
    public class MessageProcessorMicroservice
    {
        private TcpListener messageProcessorSocket;
        private TcpClient biddingProcessorSocket;
        private TcpClient auctioneerConnection;
        private IObservable<string> receiveInQueueObservable;
        private readonly List<IDisposable> subscriptions = new List<IDisposable>();
        private ConcurrentQueue<Message> messageQueue = new ConcurrentQueue<Message>();

        private const int MESSAGE_PROCESSOR_PORT = 1600;
        private const string BIDDING_PROCESSOR_HOST = "localhost";
        private const int BIDDING_PROCESSOR_PORT = 1700;

        public MessageProcessorMicroservice()
        {
            messageProcessorSocket = new TcpListener(IPAddress.Any, MESSAGE_PROCESSOR_PORT);
            messageProcessorSocket.Start();
            Console.WriteLine($"MessageProcessorMicroservice running on port: {MESSAGE_PROCESSOR_PORT}");
            Console.WriteLine("Waiting for messages to process...");

            var subject = new Subject<string>();
            receiveInQueueObservable = subject.AsObservable();

            Task.Run(async () =>
            {
                auctioneerConnection = await messageProcessorSocket.AcceptTcpClientAsync();
                Console.WriteLine("Connected to AuctioneerMicroservice");
                using var bufferReader = new StreamReader(auctioneerConnection.GetStream(), Encoding.UTF8);

                while (true)
                {
                    var receivedMessage = await bufferReader.ReadLineAsync();

                    if (receivedMessage == null)
                    {
                        Console.WriteLine("AuctioneerMicroservice disconnected.");
                        bufferReader.Close();
                        auctioneerConnection.Close();

                        subject.OnError(new Exception($"Error: AuctioneerMicroservice disconnected."));
                        break;
                    }

                    if (Message.Deserialize(Encoding.UTF8.GetBytes(receivedMessage)).Body == "final")
                    {
                        Console.WriteLine("Received final message from AuctioneerMicroservice.");
                        subject.OnCompleted();
                        break;
                    }
                    else
                    {
                        subject.OnNext(receivedMessage);
                    }
                }
            });
        }

        private void ReceiveAndProcessMessages()
        {
            var receiveInQueueSubscription = receiveInQueueObservable.Subscribe(
                onNext: msg =>
                {
                    var message = Message.Deserialize(Encoding.UTF8.GetBytes(msg));
                    Console.WriteLine($"Received {message}. Adding to queue");

                    if (!messageQueue.Contains(message))
                        messageQueue.Enqueue(message);
                },
                onCompleted: () =>
                {
                    Console.WriteLine("Finished receiving all messages. Processing queue...");
                    var sortedQueue = messageQueue.OrderBy(m => m.Timestamp).ToList();
                    messageQueue = new ConcurrentQueue<Message>(sortedQueue);

                    var finishedMessagesMessage = Message.Create(
                        $"{auctioneerConnection.Client.LocalEndPoint}",
                        "am primit tot"
                    );

                    auctioneerConnection.GetStream().Write(finishedMessagesMessage.Serialize());
                    auctioneerConnection.Close();

                    SendProcessedMessages();
                },
                onError: ex =>
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }
            );

            subscriptions.Add(receiveInQueueSubscription);
        }

        private void SendProcessedMessages()
        {
            try
            {
                biddingProcessorSocket = new TcpClient(BIDDING_PROCESSOR_HOST, BIDDING_PROCESSOR_PORT);
                Console.WriteLine("Connected to BiddingProcessorMicroservice");

                Console.WriteLine("Sending the following messages:");
                foreach (var message in messageQueue)
                {
                    Console.WriteLine(message);
                    biddingProcessorSocket.GetStream().Write(message.Serialize());
                }

                var noMoreMessages = Message.Create(
                    $"{biddingProcessorSocket.Client.LocalEndPoint}",
                    "final"
                );

                biddingProcessorSocket.GetStream().Write(noMoreMessages.Serialize());
                biddingProcessorSocket.Close();

                subscriptions.ForEach(sub => sub.Dispose());
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Cannot connect to BiddingProcessor! Error: {ex.Message}");
                messageProcessorSocket.Stop();
                Environment.Exit(1);
            }
        }

        public void Run()
        {
            ReceiveAndProcessMessages();
            Console.WriteLine("MessageProcessorMicroservice is running...");
            Console.ReadLine(); // Keep the console open
        }

        public static void Main(string[] args)
        {
            var messageProcessorMicroservice = new MessageProcessorMicroservice();
            messageProcessorMicroservice.Run();
        }
    }

    public class Message
    {
        public string Sender { get; private set; }
        public string Body { get; private set; }
        public DateTime Timestamp { get; private set; }

        private Message(string sender, string body, DateTime timestamp)
        {
            Sender = sender;
            Body = body;
            Timestamp = timestamp;
        }

        public static Message Create(string sender, string body)
        {
            return new Message(sender, body, DateTime.Now);
        }

        public static Message Deserialize(byte[] data)
        {
            var msgString = Encoding.UTF8.GetString(data);
            var parts = msgString.Split(' ', 3);
            var timestamp = DateTimeOffset.FromUnixTimeMilliseconds(long.Parse(parts[0])).DateTime;
            var sender = parts[1];
            var body = parts[2];
            return new Message(sender, body, timestamp);
        }

        public byte[] Serialize()
        {
            var timestamp = new DateTimeOffset(Timestamp).ToUnixTimeMilliseconds();
            return Encoding.UTF8.GetBytes($"{timestamp} {Sender} {Body}\n");
        }

        public override string ToString()
        {
            return $"[{Timestamp:dd-MM-yyyy HH:mm:ss}] {Sender} >>> {Body}";
        }
    }
}

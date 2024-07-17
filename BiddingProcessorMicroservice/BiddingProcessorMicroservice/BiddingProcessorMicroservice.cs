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

namespace BiddingProcessorMicroserviceApp
{
    public class BiddingProcessorMicroservice
    {
        private TcpListener biddingProcessorSocket;
        private TcpClient auctioneerSocket;
        private IObservable<string> receiveProcessedBidsObservable;
        private readonly List<IDisposable> subscriptions = new List<IDisposable>();
        private readonly ConcurrentQueue<Message> processedBidsQueue = new ConcurrentQueue<Message>();

        private const int BIDDING_PROCESSOR_PORT = 1700;
        private const int AUCTIONEER_PORT = 1500;
        private const string AUCTIONEER_HOST = "localhost";

        public BiddingProcessorMicroservice()
        {
            biddingProcessorSocket = new TcpListener(IPAddress.Any, BIDDING_PROCESSOR_PORT);
            biddingProcessorSocket.Start();

            Console.WriteLine($"BiddingProcessorMicroservice running on port: {BIDDING_PROCESSOR_PORT}");
            Console.WriteLine("Waiting for bids to finalize the auction...");

            var subject = new Subject<string>();
            receiveProcessedBidsObservable = subject.AsObservable();

            Task.Run(async () =>
            {
                var messageProcessorConnection = await biddingProcessorSocket.AcceptTcpClientAsync();
                using var bufferReader = new StreamReader(messageProcessorConnection.GetStream(), Encoding.UTF8);

                while (true)
                {
                    var receivedMessage = await bufferReader.ReadLineAsync();

                    if (receivedMessage == null)
                    {
                        bufferReader.Close();
                        messageProcessorConnection.Close();

                        subject.OnError(new Exception($"Error: MessageProcessorMicroservice disconnected."));
                        break;
                    }

                    if (Message.Deserialize(Encoding.UTF8.GetBytes(receivedMessage)).Body == "final")
                    {
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

        private void ReceiveProcessedBids()
        {
            var receiveProcessedBidsSubscription = receiveProcessedBidsObservable.Subscribe(
                onNext: msg =>
                {
                    var message = Message.Deserialize(Encoding.UTF8.GetBytes(msg));
                    Console.WriteLine(message);
                    processedBidsQueue.Enqueue(message);
                },
                onCompleted: () =>
                {
                    DecideAuctionWinner();
                },
                onError: ex =>
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }
            );

            subscriptions.Add(receiveProcessedBidsSubscription);
        }

        private void DecideAuctionWinner()
        {
            var winner = processedBidsQueue.MaxBy(msg => int.Parse(msg.Body.Split(" ")[1]));
            Console.WriteLine($"The winner is: {winner?.Sender}");

            try
            {
                auctioneerSocket = new TcpClient(AUCTIONEER_HOST, AUCTIONEER_PORT);
                using var stream = auctioneerSocket.GetStream();

                stream.Write(winner.Serialize());
                auctioneerSocket.Close();

                Console.WriteLine("Announced the winner to AuctioneerMicroservice.");
            }
            catch (Exception)
            {
                Console.WriteLine("Cannot connect to Auctioneer!");
                biddingProcessorSocket.Stop();
                Environment.Exit(1);
            }
        }

        public void Run()
        {
            ReceiveProcessedBids();
            subscriptions.ForEach(sub => sub.Dispose());
        }

        public static void Main(string[] args)
        {
            var biddingProcessorMicroservice = new BiddingProcessorMicroservice();
            biddingProcessorMicroservice.Run();
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

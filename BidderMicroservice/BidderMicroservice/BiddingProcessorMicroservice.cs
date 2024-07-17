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

namespace BidderMicroserviceApp
{
    public class BidderMicroservice
    {
        private TcpClient auctioneerSocket;
        private IObservable<string> auctioneerObservable;
        private readonly List<IDisposable> subscriptions = new List<IDisposable>();
        private const string myIdentity = "BIDDER_NECOENT";
        private readonly ConcurrentQueue<Message> bidQueue = new ConcurrentQueue<Message>();

        private const string AUCTIONEER_HOST = "localhost";
        private const int AUCTIONEER_PORT = 1500;
        private const int MAX_BID = 10000;
        private const int MIN_BID = 1000;

        public BidderMicroservice()
        {
            auctioneerSocket = new TcpClient(AUCTIONEER_HOST, AUCTIONEER_PORT);
            Console.WriteLine($"BidderMicroservice connected to auctioneer on port: {AUCTIONEER_PORT}");

            var subject = new Subject<string>();
            auctioneerObservable = subject.AsObservable();

            Task.Run(async () =>
            {
                using var stream = auctioneerSocket.GetStream();
                using var bufferReader = new StreamReader(stream, Encoding.UTF8);

                while (true)
                {
                    var receivedMessage = await bufferReader.ReadLineAsync();
                    if (receivedMessage == null)
                    {
                        bufferReader.Close();
                        auctioneerSocket.Close();

                        subject.OnError(new Exception($"Error: AuctioneerMicroservice disconnected."));
                        break;
                    }

                    subject.OnNext(receivedMessage);
                }
            });
        }

        private void MakeBid()
        {
            var random = new Random();
            var bidAmount = random.Next(MIN_BID, MAX_BID);
            var bidMessage = Message.Create(myIdentity, $"licitez {bidAmount}");
            bidQueue.Enqueue(bidMessage);
        }

        private void SendBids()
        {
            var bidSubscription = auctioneerObservable.Subscribe(
                onNext: async msg =>
                {
                    var message = Message.Deserialize(Encoding.UTF8.GetBytes(msg));
                    Console.WriteLine(message);

                    MakeBid();

                    if (bidQueue.TryDequeue(out var bid))
                    {
                        var stream = auctioneerSocket.GetStream();
                        await stream.WriteAsync(bid.Serialize(), 0, bid.Serialize().Length);
                    }
                },
                onError: ex =>
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }
            );

            subscriptions.Add(bidSubscription);
        }

        public void Run()
        {
            SendBids();
        }

        public static void Main(string[] args)
        {
            var bidderMicroservice = new BidderMicroservice();
            bidderMicroservice.Run();
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

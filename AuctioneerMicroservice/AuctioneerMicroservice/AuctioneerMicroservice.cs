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

namespace AuctioneerMicroserviceApp
{
    public class AuctioneerMicroservice
    {
        private TcpListener auctioneerSocket;
        private TcpClient messageProcessorSocket;
        private IObservable<string> receiveBidsObservable;
        private readonly List<IDisposable> subscriptions = new List<IDisposable>();
        private readonly ConcurrentQueue<Message> bidQueue = new ConcurrentQueue<Message>();
        private readonly List<TcpClient> bidderConnections = new List<TcpClient>();

        private const string MESSAGE_PROCESSOR_HOST = "localhost";
        private const int MESSAGE_PROCESSOR_PORT = 1600;
        private const int AUCTIONEER_PORT = 1500;

        public AuctioneerMicroservice()
        {
            auctioneerSocket = new TcpListener(IPAddress.Any, AUCTIONEER_PORT);
            auctioneerSocket.Start();

            Console.WriteLine($"AuctioneerMicroservice running on port: {AUCTIONEER_PORT}");
            Console.WriteLine("Waiting for bids from bidders...");

            var subject = new Subject<string>();
            receiveBidsObservable = subject.AsObservable();

            Task.Run(async () =>
            {
                while (true)
                {
                    try
                    {
                        var bidderConnection = await auctioneerSocket.AcceptTcpClientAsync();
                        bidderConnections.Add(bidderConnection);

                        using var bufferReader = new StreamReader(bidderConnection.GetStream(), Encoding.UTF8);
                        var receivedMessage = await bufferReader.ReadLineAsync();

                        if (receivedMessage == null)
                        {
                            bufferReader.Close();
                            bidderConnection.Close();
                            subject.OnError(new Exception($"Error: Bidder disconnected."));
                            break;
                        }

                        subject.OnNext(receivedMessage);
                    }
                    catch (SocketException e) when (e.SocketErrorCode == SocketError.TimedOut)
                    {
                        subject.OnCompleted();
                        break;
                    }
                }
            });
        }

        private void ReceiveBids()
        {
            var receiveBidsSubscription = receiveBidsObservable.Subscribe(
                onNext: msg =>
                {
                    var message = Message.Deserialize(Encoding.UTF8.GetBytes(msg));
                    Console.WriteLine(message);
                    bidQueue.Enqueue(message);
                },
                onCompleted: () =>
                {
                    Console.WriteLine("Auction ended! Sending bids for processing...");
                    ForwardBids();
                },
                onError: ex =>
                {
                    Console.WriteLine($"Error: {ex.Message}");
                }
            );

            subscriptions.Add(receiveBidsSubscription);
        }

        private void ForwardBids()
        {
            try
            {
                messageProcessorSocket = new TcpClient(MESSAGE_PROCESSOR_HOST, MESSAGE_PROCESSOR_PORT);
                var stream = messageProcessorSocket.GetStream();

                foreach (var message in bidQueue)
                {
                    stream.Write(message.Serialize(), 0, message.Serialize().Length);
                }

                Console.WriteLine("All bids sent to MessageProcessor.");
                var bidEndMessage = Message.Create($"{((IPEndPoint)messageProcessorSocket.Client.LocalEndPoint)}", "final");
                stream.Write(bidEndMessage.Serialize(), 0, bidEndMessage.Serialize().Length);

                using var bufferReader = new StreamReader(messageProcessorSocket.GetStream(), Encoding.UTF8);
                bufferReader.ReadLine();

                messageProcessorSocket.Close();
                FinishAuction();
            }
            catch (Exception)
            {
                Console.WriteLine("Cannot connect to MessageProcessor!");
                auctioneerSocket.Stop();
                Environment.Exit(1);
            }
        }

        private void FinishAuction()
        {
            try
            {
                var biddingProcessorConnection = auctioneerSocket.AcceptTcpClient();
                using var bufferReader = new StreamReader(biddingProcessorConnection.GetStream(), Encoding.UTF8);

                var receivedMessage = bufferReader.ReadLine();
                var result = Message.Deserialize(Encoding.UTF8.GetBytes(receivedMessage));
                var winningPrice = int.Parse(result.Body.Split(" ")[1]);
                Console.WriteLine($"Auction result received from BiddingProcessor: {result.Sender} won with price: {winningPrice}");

                var winningMessage = Message.Create(auctioneerSocket.LocalEndpoint.ToString(), $"Auction won! Winning price: {winningPrice}");
                var losingMessage = Message.Create(auctioneerSocket.LocalEndpoint.ToString(), "Auction lost...");

                foreach (var connection in bidderConnections)
                {
                    using var writer = new StreamWriter(connection.GetStream(), Encoding.UTF8) { AutoFlush = true };
                    if (connection.Client.RemoteEndPoint.ToString() == result.Sender)
                    {
                        writer.WriteLine(Encoding.UTF8.GetString(winningMessage.Serialize()));
                    }
                    else
                    {
                        writer.WriteLine(Encoding.UTF8.GetString(losingMessage.Serialize()));
                    }

                    connection.Close();
                }
            }
            catch (Exception)
            {
                Console.WriteLine("Cannot connect to BiddingProcessor!");
                auctioneerSocket.Stop();
                Environment.Exit(1);
            }

            subscriptions.ForEach(sub => sub.Dispose());
        }

        public void Run()
        {
            ReceiveBids();
        }

        public static void Main(string[] args)
        {
            var auctioneerMicroservice = new AuctioneerMicroservice();
            auctioneerMicroservice.Run();
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

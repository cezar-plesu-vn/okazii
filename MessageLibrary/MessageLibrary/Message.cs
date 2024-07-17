using System;
using System.Text;
using System.Text.RegularExpressions;
using System.Globalization;

namespace MessageLibrary
{
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

        public static Message Deserialize(byte[] msg)
        {
            string msgString = Encoding.UTF8.GetString(msg);

            if (msgString.Contains("final") || Regex.IsMatch(msgString, @"\d+\.\d+\.\d+\.\d+"))
            {
                string[] parts = msgString.Split(new char[] { ' ' }, 3);
                long timestamp = long.Parse(parts[0]);
                string sender = parts[1];
                string body = parts[2];
                return new Message(sender, body, DateTimeOffset.FromUnixTimeMilliseconds(timestamp).DateTime);
            }

            int openParenIndex = msgString.IndexOf("(");
            int closeParenIndex = msgString.IndexOf(")");

            if (openParenIndex == -1 || closeParenIndex == -1 || closeParenIndex <= openParenIndex)
            {
                throw new FormatException("Invalid message format");
            }

            string timestampStr = msgString.Substring(0, openParenIndex).Trim();
            string senderPart = "(" + msgString.Substring(openParenIndex + 1, closeParenIndex - openParenIndex - 1) + ")";
            string bodyPart = msgString.Substring(closeParenIndex + 2).Trim();

            long timestampMs = long.Parse(timestampStr);
            return new Message(senderPart, bodyPart, DateTimeOffset.FromUnixTimeMilliseconds(timestampMs).DateTime);
        }

        public byte[] Serialize()
        {
            return Encoding.UTF8.GetBytes($"{((DateTimeOffset)Timestamp).ToUnixTimeMilliseconds()} {Sender} {Body}\n");
        }

        public override string ToString()
        {
            string dateString = Timestamp.ToString("dd-MM-yyyy HH:mm:ss", CultureInfo.InvariantCulture);
            return $"[{dateString}] {Sender} >>> {Body}";
        }
    }
}

using System;
using MessageLibrary;

public class Program
{
    public static void Main(string[] args)
    {
        Message msg = Message.Create("localhost:4848", "test mesaj");
        Console.WriteLine(msg);

        byte[] serialized = msg.Serialize();
        Message deserialized = Message.Deserialize(serialized);
        Console.WriteLine(deserialized);
    }
}
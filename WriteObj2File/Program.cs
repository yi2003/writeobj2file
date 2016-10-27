using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace WriteObj2File
{


    public class ObjFileTool
    {
        private BinaryWriter writer;


        public ObjFileTool(Stream stream)
        {
            writer = new BinaryWriter(stream);
        }

        public PullMessageRequest ReadFromStream(Stream stream)
        {
            using (var reader = new BinaryReader(stream))
            {
                var request = new PullMessageRequest();
                request.ConsumerId = reader.ReadString();
                request.ConsumerGroup = reader.ReadString();
                request.MessageQueue = new MessageQueue(reader.ReadString(), reader.ReadString(), reader.ReadInt32());
                request.Tags = reader.ReadString();
                request.QueueOffset = reader.ReadInt64();
                request.SuspendPullRequestMilliseconds = reader.ReadInt64();
                return request;
            }
        }



        public void Write2Stream(PullMessageRequest request)
        {
            writer.Write(request.ConsumerId);
            writer.Write(request.ConsumerGroup);
            writer.Write(request.MessageQueue.v1);
            writer.Write(request.MessageQueue.v2);
            writer.Write(request.MessageQueue.v3);
            writer.Write(request.Tags);
            writer.Write(request.QueueOffset);
            writer.Write(request.SuspendPullRequestMilliseconds);



        }


        public void Flush()
        {
            this.writer.Flush();
        }



    }

    public class PullMessageRequest
    {
        public string ConsumerId { get; set; }
        public string ConsumerGroup { get; set; }
        public MessageQueue MessageQueue { get; set; }
        public string Tags { get; set; }
        public long QueueOffset { get; set; }
        public long SuspendPullRequestMilliseconds { get; set; }
    }

    public class MessageQueue
    {
        public string v1 { get; set; }
        public string v2 { get; set; }
        public int v3 { get; set; }

        public MessageQueue(string v1, string v2, int v3)
        {
            this.v1 = v1;
            this.v2 = v2;
            this.v3 = v3;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            //test1();

           // testWrite();
            TestRead();
        }

        private static void TestRead()
        {
            
            var fileStream = new FileStream("test.txt", FileMode.Open);
            var readStream= new BufferedStream(fileStream);

            var tool = new ObjFileTool(readStream);

            var request = tool.ReadFromStream(readStream);

            Console.WriteLine(request.ConsumerId);
            Console.WriteLine(request.MessageQueue.v1);

        }

        private static void testWrite()
        {
            var fileStream = new FileStream("test.txt", FileMode.Append);
            Console.WriteLine("p1:"+fileStream.Position);
            var writeStream = new BufferedStream(fileStream);

            var tool = new ObjFileTool(writeStream);
            var requests = new List<PullMessageRequest>();
            requests.Add(new PullMessageRequest()
            {
                ConsumerId = "aaaa",
                ConsumerGroup = "dddd",
                MessageQueue = new MessageQueue("hello","world",233),
                QueueOffset = 100,
                SuspendPullRequestMilliseconds = 2000,
                Tags = "Tags"
            });
            foreach (var r in requests)
            {
                tool.Write2Stream(r);
            }
            tool.Flush();
            Console.WriteLine("p2:"+fileStream.Position);

            fileStream.Close();

        }

        private static void test1()
        {
            var fileStream = new FileStream("test.txt", FileMode.OpenOrCreate);
            var writeStream = new BufferedStream(fileStream);

            var tool = new ObjFileTool(writeStream);

            var requests = new List<PullMessageRequest>();
            foreach (var r in requests)
            {
                tool.Write2Stream(r);
            }
            tool.Flush();
        }
    }
}

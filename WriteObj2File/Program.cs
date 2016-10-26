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

            //consumerid
            var consumerIdBytes = Encoding.UTF8.GetBytes(request.ConsumerId);
            writer.Write(consumerIdBytes.Length);
            writer.Write(consumerIdBytes);


            //consumer group
            var consumerGroupBytes = Encoding.UTF8.GetBytes(request.ConsumerGroup);
            writer.Write(consumerGroupBytes.Length);
            writer.Write(consumerGroupBytes);


            //queque v1
            //var v1 = Encoding.UTF8.GetBytes(request.MessageQueue.V1);
            //writer.Write(consumerGroupBytes.Length);
            //writer.Write(consumerGroupBytes);



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
        protected string v1;
        private string v2;
        private int v3;

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

            test2();
        }

        private static void test2()
        {
            var fileStream = new FileStream("test.txt", FileMode.Append);
            Console.WriteLine("p1:"+fileStream.Position);
            var writeStream = new BufferedStream(fileStream);

            var tool = new ObjFileTool(writeStream);
            var requests = new List<PullMessageRequest>();
            requests.Add(new PullMessageRequest()
            {
                ConsumerId = "aaaa",
                ConsumerGroup = "dddd"
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

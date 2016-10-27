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

        private Stream stream;

        public ObjFileTool(Stream stream)
        {
            this.stream = stream;
            writer = new BinaryWriter(stream);
            
        }

        public PullMessageRequest ReadFromStream()
        {
            using (var reader = new BinaryReader(stream))
            {
                var request = new PullMessageRequest();
                request.Id = reader.ReadInt32();
                request.ConsumerId = reader.ReadString();
                request.ConsumerGroup = reader.ReadString();
                request.MessageQueue = new MessageQueue(reader.ReadString(), reader.ReadString(), reader.ReadInt32());
                request.Tags = reader.ReadString();
                request.QueueOffset = reader.ReadInt64();
                request.SuspendPullRequestMilliseconds = reader.ReadInt64();
                return request;
            }
        }


        public PullMessageRequest ReadFromStreamByPosition(long position)
        {
            stream.Position = position;
            using (var reader = new BinaryReader(stream))
            {
                var request = new PullMessageRequest();
                request.Id = reader.ReadInt32();
                request.ConsumerId = reader.ReadString();
                request.ConsumerGroup = reader.ReadString();
                request.MessageQueue = new MessageQueue(reader.ReadString(), reader.ReadString(), reader.ReadInt32());
                request.Tags = reader.ReadString();
                request.QueueOffset = reader.ReadInt64();
                request.SuspendPullRequestMilliseconds = reader.ReadInt64();
                return request;
            }
        }

        public MessageRequest ReadFromStreamByBytes()
        {
            var r = new MessageRequest();
            using (var reader = new BinaryReader(stream))
            {
                var start = 0;
                var recordBuffer = new byte[4];
                reader.Read(recordBuffer, start, 4);//int length

                int i = 0;
                var id = DecodeInt(recordBuffer, 0, out i);

                r.Id = id;

             //   start += 4;
                var consumerIdLenBuffer= new byte[4];
                reader.Read(consumerIdLenBuffer, start, 4);//int length

               // start += 4;
                var consumerIdLen = DecodeInt(consumerIdLenBuffer, 0, out i);
                var consumerIdBuffer = new byte[consumerIdLen];
                reader.Read(consumerIdBuffer, start, consumerIdLen);//int length

                r.ConsumerId = DecodeString(consumerIdBuffer, 0, out i);
               // start += consumerIdLen;


                var consumerGroupLenBuffer = new byte[4];
                reader.Read(consumerGroupLenBuffer, start, 4);//int length
               // start += 4;


                var consumerGroupLen = DecodeInt(consumerGroupLenBuffer, 0, out i);
                var consumerGroupBuffer = new byte[consumerGroupLen];
                reader.Read(consumerGroupBuffer, start, consumerGroupLen);//int length

                r.ConsumerGroup = DecodeString(consumerGroupBuffer, 0, out i);
            }

            return r;
        }

        public void WriteByptes2Stream( MessageRequest request)
        {
         

            var idByptes = BitConverter.GetBytes(request.Id);
            var idBytpesLen =idByptes.Length;

            var consumerIdBytes = Encoding.UTF8.GetBytes(request.ConsumerId);
            var consumerIdLen = consumerIdBytes.Length;


            var csonsumerGroupByptes = Encoding.UTF8.GetBytes(request.ConsumerGroup);
            var csonsumerGroupByptesLen = csonsumerGroupByptes.Length;



            // var requestL
            var recordBuffer = new byte[idBytpesLen +4+ consumerIdLen+ 4+csonsumerGroupByptesLen];

            var startPosition = 0;
            Buffer.BlockCopy(idByptes, 0, recordBuffer, startPosition, idBytpesLen);
            startPosition += idBytpesLen;

            Buffer.BlockCopy(BitConverter.GetBytes(consumerIdLen), 0, recordBuffer, startPosition, 4);
            startPosition += 4;

            Buffer.BlockCopy(consumerIdBytes, 0, recordBuffer, startPosition, consumerIdLen);
            startPosition += consumerIdLen;

           
            Buffer.BlockCopy(BitConverter.GetBytes(csonsumerGroupByptesLen), 0, recordBuffer, startPosition, 4);
            startPosition += 4;

            Buffer.BlockCopy(csonsumerGroupByptes, 0, recordBuffer, startPosition, csonsumerGroupByptesLen);
            //startPosition += csonsumerGroupByptesLen;

            writer.Write(recordBuffer);

        }


        public static string DecodeString(byte[] sourceBuffer, int startOffset, out int nextStartOffset)
        {
            return Encoding.UTF8.GetString(DecodeBytes(sourceBuffer, startOffset, out nextStartOffset));
        }

        public static byte[] DecodeBytes(byte[] sourceBuffer, int startOffset, out int nextStartOffset)
        {
            //var lengthBytes = new byte[4];
            //Buffer.BlockCopy(sourceBuffer, startOffset, lengthBytes, 0, 4);
            //startOffset += 4;

            var length = sourceBuffer.Length;
            var dataBytes = new byte[length];
            Buffer.BlockCopy(sourceBuffer, startOffset, dataBytes, 0, length);
            startOffset += length;

            nextStartOffset = startOffset;

            return dataBytes;
        }

        public static int DecodeInt(byte[] sourceBuffer, int startOffset, out int nextStartOffset)
        {
            var intBytes = new byte[4];
            Buffer.BlockCopy(sourceBuffer, startOffset, intBytes, 0, 4);
            nextStartOffset = startOffset + 4;
            return BitConverter.ToInt32(intBytes, 0);
        }


        public void Write2Stream(PullMessageRequest request)
        {
            writer.Write(request.Id);
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

    public class MessageRequest
    {
        public int Id { get; set; }

        public string ConsumerId { get; set; }

        public string ConsumerGroup { get; set; }

        public long Position
        {
            get; set;
        }
    }

    public class PullMessageRequest
    {
        public long Position { get; set; }
        public int Id { get; set; }
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

           //testWrite();
            //TestRead();

           //TestWritebytes();

            TestReadbytes();
            Console.Read();
        }


        private static void TestReadbytes()
        {
            var fileStream = new FileStream("testbytes.txt", FileMode.Open);

            var readStream = new BufferedStream(fileStream);
            var tool = new ObjFileTool(readStream);

            readStream.Position = 26;
            var request = tool.ReadFromStreamByBytes();


            Console.WriteLine(request.Id);
            Console.WriteLine(request.ConsumerId);
            Console.WriteLine(request.ConsumerGroup);
            Console.Read();
        }

        private static void TestWritebytes()
        {
            var fileStream = new FileStream("testbytes.txt", FileMode.Append);
            var writeStream = new BufferedStream(fileStream);

            var tool = new ObjFileTool(writeStream);
            var requests = new List<MessageRequest>();
            requests.Add(new MessageRequest()
            {
                Id = 7,
                ConsumerId = "aaaabbbbcc",
                ConsumerGroup = "dddd",
             
            });

            requests.Add(new MessageRequest()
            {
                Id = 2,
                ConsumerId = "ccc",
                ConsumerGroup = "eee",

            });
            foreach (var r in requests)
            {
               // var position = fileStream.Position;
                tool.WriteByptes2Stream(r);
                tool.Flush();
                r.Position = fileStream.Position;
                //47
                //94
                Console.WriteLine("position:" + r.Position);
            }
            //   Console.WriteLine("p2:"+fileStream.Position);

            fileStream.Close();
            //

        }

        private static void TestRead()
        {
            
            var fileStream = new FileStream("test.txt", FileMode.Open);
            var readStream= new BufferedStream(fileStream);
            var tool = new ObjFileTool(readStream);

            var request = tool.ReadFromStreamByPosition(51);


            Console.WriteLine(request.Id);
            Console.WriteLine(request.ConsumerId);
            Console.WriteLine(request.MessageQueue.v1);
            Console.Read();
        }

        private static void testWrite()
        {
            var fileStream = new FileStream("test.txt", FileMode.Append);
        //    Console.WriteLine("p1:"+fileStream.Position);
            var writeStream = new BufferedStream(fileStream);

            var tool = new ObjFileTool(writeStream);
            var requests = new List<PullMessageRequest>();
            requests.Add(new PullMessageRequest()
            {
                Id=1,
                ConsumerId = "aaaa",
                ConsumerGroup = "dddd",
                MessageQueue = new MessageQueue("hello","world",233),
                QueueOffset = 100,
                SuspendPullRequestMilliseconds = 2000,
                Tags = "Tags"
            });


            requests.Add(new PullMessageRequest()
            {
                Id = 2,
                ConsumerId = "cccc",
                ConsumerGroup = "eeee",
                MessageQueue = new MessageQueue("hello", "world", 233),
                QueueOffset = 100,
                SuspendPullRequestMilliseconds = 2000,
                Tags = "Tags"
            });
            foreach (var r in requests)
            {
                tool.Write2Stream(r);
                tool.Flush();
                r.Position = fileStream.Position;
                //47
                //94
                Console.WriteLine("position:"+r.Position);
            }
         //   Console.WriteLine("p2:"+fileStream.Position);

            fileStream.Close();
            Console.Read();
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

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using RedisSharp;

namespace RedisTest
{
    class Program
    {
        static int nPassed = 0;
        static int nFailed = 0;

        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        static async Task MainAsync(string[] args)
        {
            Redis redis;
            string s;
            int i;

            if (args.Length >= 2)
                redis = new Redis(args[0], Convert.ToInt16(args[1]));
            else if (args.Length >= 1)
                redis = new Redis(args[0]);
            else
                redis = new Redis();


            await redis.LeftPushAsync("test", "abcd");
            var result = await redis.BlockingLeftPopAsync("test", 1);


            redis.Set("foo", "bar");
            redis.FlushAll();
            assert((i = redis.Keys.Length) == 0, "there should be no keys but there were {0}", i);
            redis.Set("foo", "bar");
            assert((i = redis.Keys.Length) == 1, "there should be one key but there were {0}", i);
            redis.Set("foo bär", "bär foo");
            assert((i = redis.Keys.Length) == 2, "there should be two keys but there were {0}", i);

            assert(redis.TypeOf("foo") == KeyType.String, "type is not string");
            redis.Set("bar", "foo");

            byte[][] arr = redis.MGet("foo", "bar", "foo bär");
            assert(arr.Length == 3, "expected 3 values");
            assert((s = Encoding.UTF8.GetString(arr[0])) == "bar",
                "expected \"foo\" to be \"bar\", got \"{0}\"", s);
            assert((s = Encoding.UTF8.GetString(arr[1])) == "foo",
                "expected \"bar\" to be \"foo\", got \"{0}\"", s);
            assert((s = Encoding.UTF8.GetString(arr[2])) == "bär foo",
                "expected \"foo bär\" to be \"bär foo\", got \"{0}\"", s);

            redis["{one}"] = "world";
            assert(redis.GetSet("{one}", "newvalue") == "world", "GetSet failed");
            assert(redis.Rename("{one}", "two"), "failed to rename");
            assert(!redis.Rename("{one}", "{one}"), "should have sent an error on rename");
            redis.Set("binary", new byte[] { 0x00, 0x8F });
            assert((i = redis.Get("binary").Length) == 2, "expected 2 bytes, got {0}", i);
            redis.Db = 10;
            redis.Set("foo", "diez");
            assert((s = redis.GetString("foo")) == "diez", "got {0}", s);
            assert(redis.Remove("foo"), "could not remove foo");
            redis.Db = 0;
            assert(redis.GetString("foo") == "bar", "foo was not bar");
            assert(redis.ContainsKey("foo"), "there is no foo");
            assert(redis.Remove("foo", "bar") == 2, "did not remove two keys");
            assert(!redis.ContainsKey("foo"), "foo should be gone.");
            redis.Save();
            redis.BackgroundSave();
            Console.WriteLine("Last save: {0}", redis.LastSave);
            //r.Shutdown ();

            var info = redis.GetInfo();
            foreach (var k in info.Keys)
            {
                Console.WriteLine("{0} -> {1}", k, info[k]);
            }

            var dict = new Dictionary<string, string>();
            dict["hello"] = "world";
            dict["goodbye"] = "my dear";
            dict["schön"] = "grün";
            redis.Set(dict);
            assert((s = redis.GetString("hello")) == "world", "got \"{0}\"", s);
            assert((s = redis.GetString("goodbye")) == "my dear", "got \"{0}\"", s);
            assert((s = redis.GetString("schön")) == "grün", "got \"{0}\"", s);

            redis.RightPush("alist", "avalue");
            redis.RightPush("alist", "another value");
            assert(redis.ListLength("alist") == 2, "List length should have been 2");

            string value = Encoding.UTF8.GetString(redis.ListIndex("alist", 1));
            if (!value.Equals("another value"))
                Console.WriteLine("error: Received {0} and should have been 'another value'", value);
            value = Encoding.UTF8.GetString(redis.LeftPop("alist"));
            if (!value.Equals("avalue"))
                Console.WriteLine("error: Received {0} and should have been 'avalue'", value);
            if (redis.ListLength("alist") != 1)
                Console.WriteLine("error: List should have one element after pop");
            redis.LeftPush("alist", "yet another value");
            SortOptions so = new SortOptions();
            so.Key = "alist";
            so.Lexographically = true;
            assert((s = Encoding.UTF8.GetString(redis.Sort(so)[0])) == "another value",
                "expected Sort result \"another value\", got \"" + s + "\"");
            assert((i = redis.Sort("alist", "alist", new object[] { "ALPHA" }).Length) == 2,
                "expected Sort result 2, got {0}", i);
            byte[][] values = redis.ListRange("alist", 0, 1);
            assert(Encoding.UTF8.GetString(values[0]).Equals("another value"),
                "range did not return the right values");

            assert(redis.AddToSet("FOO", Encoding.UTF8.GetBytes("BAR")), "problem adding to set");
            assert(redis.AddToSet("FOO", Encoding.UTF8.GetBytes("BAZ")), "problem adding to set");
            assert(redis.AddToSet("FOO", "Hoge"), "problem adding string to set");
            assert(redis.CardinalityOfSet("FOO") == 3, "cardinality should have been 3 after adding 3 items to set");
            assert(redis.IsMemberOfSet("FOO", Encoding.UTF8.GetBytes("BAR")), "BAR should have been in the set");
            assert(redis.IsMemberOfSet("FOO", "BAR"), "BAR should have been in the set");
            byte[][] members = redis.GetMembersOfSet("FOO");
            assert(members.Length == 3, "set should have had 3 members");

            assert(redis.RemoveFromSet("FOO", "Hoge"), "should have removed Hoge from set");
            assert(!redis.RemoveFromSet("FOO", "Hoge"), "Hoge should not have existed to be removed");
            assert(2 == redis.GetMembersOfSet("FOO").Length, "set should have 2 members after removing Hoge");

            assert(redis.AddToSet("BAR", Encoding.UTF8.GetBytes("BAR")), "problem adding to set");
            assert(redis.AddToSet("BAR", Encoding.UTF8.GetBytes("ITEM1")), "problem adding to set");
            assert(redis.AddToSet("BAR", Encoding.UTF8.GetBytes("ITEM2")), "problem adding string to set");

            assert(redis.GetUnionOfSets("FOO", "BAR").Length == 4, "resulting union should have 4 items");
            assert(1 == redis.GetIntersectionOfSets("FOO", "BAR").Length, "resulting intersection should have 1 item");
            assert(1 == redis.GetDifferenceOfSets("FOO", "BAR").Length, "resulting difference should have 1 item");
            assert(2 == redis.GetDifferenceOfSets("BAR", "FOO").Length, "resulting difference should have 2 items");

            byte[] itm = redis.GetRandomMemberOfSet("FOO");
            assert(null != itm, "GetRandomMemberOfSet should have returned an item");
            assert(redis.MoveMemberToSet("FOO", "BAR", itm), "data within itm should have been moved to set BAR");

            redis.FlushDb();
            assert((i = redis.Keys.Length) == 0, "there should be no keys but there were {0}", i);

            redis.Dispose();

            Console.WriteLine("\nPassed tests: {0}", nPassed);
            if (nFailed > 0)
                Console.WriteLine("Failed tests: {0}", nFailed);

            Console.Read();
        }

        static void assert(bool condition, string message, params object[] args)
        {
            if (condition)
            {
                nPassed++;
            }
            else
            {
                nFailed++;
                Console.Error.WriteLine("Failed: " + message, args);
            }
        }
    }
}

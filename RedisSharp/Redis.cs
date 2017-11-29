//
// redis-sharp.cs: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Miguel de Icaza (miguel@gnome.org)
//
// Copyright 2010 Novell, Inc.
//
// Licensed under the same terms of reddis: new BSD license.
//
#define DEBUG
using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RedisSharp
{
    public class Redis : IDisposable
    {
        private Socket _socket;
        private BufferedStream _bstream;
        private StreamReader _streamReader;
        private readonly ArrayPool<byte> _arrayPool;

        public Redis(string host, int port, ArrayPool<byte> arrayPool)
        {
            Host = host ?? throw new ArgumentNullException(nameof(host));
            Port = port;
            SendTimeout = -1;
            _arrayPool = arrayPool ?? throw new ArgumentNullException(nameof(arrayPool));
        }

        public Redis(string host, int port) : this(host, port, ArrayPool<byte>.Shared)
        {

        }

        public Redis(string host) : this(host, 6379)
        {
        }

        public Redis() : this("localhost", 6379)
        {
        }

        public string Host { get; }

        public int Port { get; }

        public int RetryTimeout { get; set; }

        public int RetryCount { get; set; }

        public int SendTimeout { get; set; }

        public string Password { get; set; }

        private int _db;
        public int Db
        {
            get => _db;

            set
            {
                _db = value;
                SendExpectSuccess("SELECT", _db);
            }
        }

        public string this[string key]
        {
            get => GetString(key);
            set => Set(key, value);
        }

        public void Set(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            Set(key, Encoding.UTF8.GetBytes(value));
        }

        public void Set(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            if (value.Length > 1073741824)
                throw new ArgumentException("value exceeds 1G", nameof(value));

            if (!SendDataCommand(value, "SET", key))
                throw new Exception("Unable to connect");
            ExpectSuccess();
        }

        public bool SetNx(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            return SetNx(key, Encoding.UTF8.GetBytes(value));
        }

        public bool SetNx(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            if (value.Length > 1073741824)
                throw new ArgumentException("value exceeds 1G", nameof(value));

            return SendDataExpectInt(value, "SETNX", key) > 0;
        }

        public void Set(IDictionary<string, string> dict)
        {
            if (dict == null)
                throw new ArgumentNullException(nameof(dict));

            Set(dict.ToDictionary(k => k.Key, v => Encoding.UTF8.GetBytes(v.Value)));
        }

        public void Set(IDictionary<string, byte[]> dict)
        {
            if (dict == null)
                throw new ArgumentNullException(nameof(dict));

            MSet(dict.Keys.ToArray(), dict.Values.ToArray());
        }

        public void MSet(string[] keys, byte[][] values)
        {
            if (keys.Length != values.Length)
                throw new ArgumentException("keys and values must have the same size");

            var nl = Encoding.UTF8.GetBytes("\r\n");
            var ms = new MemoryStream();

            for (var i = 0; i < keys.Length; i++)
            {
                var key = Encoding.UTF8.GetBytes(keys[i]);
                var val = values[i];
                var kLength = Encoding.UTF8.GetBytes("$" + key.Length + "\r\n");
                var k = Encoding.UTF8.GetBytes(keys[i] + "\r\n");
                var vLength = Encoding.UTF8.GetBytes("$" + val.Length + "\r\n");
                ms.Write(kLength, 0, kLength.Length);
                ms.Write(k, 0, k.Length);
                ms.Write(vLength, 0, vLength.Length);
                ms.Write(val, 0, val.Length);
                ms.Write(nl, 0, nl.Length);
            }

            SendDataResp(ms.ToArray(), "*" + (keys.Length * 2 + 1) + "\r\n$4\r\nMSET\r\n");
            ExpectSuccess();
        }

        public byte[] Get(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return SendExpectData("GET", key);
        }

        public string GetString(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return Encoding.UTF8.GetString(Get(key));
        }

        public byte[][] Sort(SortOptions options)
        {
            return Sort(options.Key, options.StoreInKey, options.ToArgs());
        }

        public byte[][] Sort(string key, string destination, params object[] options)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));

            var offset = string.IsNullOrEmpty(destination) ? 1 : 3;
            var args = new object[offset + options.Length];

            args[0] = key;
            Array.Copy(options, 0, args, offset, options.Length);
            if (offset == 1)
                return SendExpectDataArray("SORT", args);
            args[1] = "STORE";
            args[2] = destination;
            var n = SendExpectInt("SORT", args);
            return new byte[n][];
        }

        public byte[] GetSet(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            if (value == null)
                throw new ArgumentNullException(nameof(value));

            if (value.Length > 1073741824)
                throw new ArgumentException("value exceeds 1G", nameof(value));

            if (!SendDataCommand(value, "GETSET", key))
                throw new Exception("Unable to connect");

            return ReadData();
        }

        public string GetSet(string key, string value)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            if (value == null)
                throw new ArgumentNullException(nameof(value));
            return Encoding.UTF8.GetString(GetSet(key, Encoding.UTF8.GetBytes(value)));
        }

        private string ReadLine()
        {
            var sb = new StringBuilder();
            int c;

            while ((c = _bstream.ReadByte()) != -1)
            {
                if (c == '\r')
                    continue;
                if (c == '\n')
                    break;
                sb.Append((char)c);
            }
            return sb.ToString();
        }

        private async Task<string> ReadLineAsync(CancellationToken cancellationToken)
        {
            var sb = new StringBuilder();
            var buffer = _arrayPool.Rent(1);

            try
            {
                while (await _bstream.ReadAsync(buffer, 0, 1, cancellationToken) != -1)
                {
                    var c = buffer[0];

                    if (c == '\r')
                        continue;
                    if (c == '\n')
                        break;
                    sb.Append((char) c);
                }
                return sb.ToString();
            }
            finally
            {
                _arrayPool.Return(buffer);
            }
        }

        private void Connect()
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,
                SendTimeout = SendTimeout
            };
            _socket.Connect(Host, Port);
            if (!_socket.Connected)
            {
                _socket.Close();
                _socket = null;
                return;
            }
            _bstream = new BufferedStream(new NetworkStream(_socket), 16 * 1024);
            _streamReader = new StreamReader(_bstream);

            if (Password != null)
                SendExpectSuccess("AUTH", Password);
        }

        private async Task ConnectAsync()
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,
                SendTimeout = SendTimeout
            };
            await _socket.ConnectAsync(Host, Port);
            if (!_socket.Connected)
            {
                _socket.Close();
                _socket = null;
                return;
            }
            _bstream = new BufferedStream(new NetworkStream(_socket), 16 * 1024);
            _streamReader = new StreamReader(_bstream);

            if (Password != null)
                SendExpectSuccess("AUTH", Password);
        }
        private readonly byte[] _endData = { (byte)'\r', (byte)'\n' };

        private bool SendDataCommand(byte[] data, string cmd, params object[] args)
        {
            var resp = "*" + (1 + args.Length + 1) + "\r\n";
            resp += "$" + cmd.Length + "\r\n" + cmd + "\r\n";
            foreach (var arg in args)
            {
                var argStr = arg.ToString();
                var argStrLength = Encoding.UTF8.GetByteCount(argStr);
                resp += "$" + argStrLength + "\r\n" + argStr + "\r\n";
            }
            resp += "$" + data.Length + "\r\n";

            return SendDataResp(data, resp);
        }

        private Task<bool> SendDataCommandAsync(byte[] data, string cmd, params object[] args)
        {
            var resp = "*" + (1 + args.Length + 1) + "\r\n";
            resp += "$" + cmd.Length + "\r\n" + cmd + "\r\n";
            foreach (var arg in args)
            {
                var argStr = arg.ToString();
                var argStrLength = Encoding.UTF8.GetByteCount(argStr);
                resp += "$" + argStrLength + "\r\n" + argStr + "\r\n";
            }
            resp += "$" + data.Length + "\r\n";

            return SendDataRespAsync(data, resp);
        }


        private bool SendDataResp(byte[] data, string resp)
        {
            if (_socket == null)
                Connect();
            if (_socket == null)
                return false;

            var r = Encoding.UTF8.GetBytes(resp);
            try
            {
                Log("C", resp);
                _socket.Send(r);
                if (data != null)
                {
                    _socket.Send(data);
                    _socket.Send(_endData);
                }
            }
            catch (SocketException)
            {
                // timeout;
                _socket.Close();
                _socket = null;

                return false;
            }
            return true;
        }

        private async Task<bool> SendDataRespAsync(byte[] data, string resp)
        {
            if (_socket == null)
                await ConnectAsync();
            if (_socket == null)
                return false;

            var r = Encoding.UTF8.GetBytes(resp);
            try
            {
                Log("C", resp);
                _socket.Send(r);
                if (data != null)
                {
                    await _socket.SendAsync(new ArraySegment<byte>(data), SocketFlags.None);
                    await _socket.SendAsync(new ArraySegment<byte>(_endData), SocketFlags.None);
                }
            }
            catch (SocketException)
            {
                // timeout;
                _socket.Close();
                _socket = null;

                return false;
            }
            return true;
        }

        private bool SendCommand(string cmd, params object[] args)
        {
            if (_socket == null)
                Connect();
            if (_socket == null)
                return false;

            var resp = "*" + (1 + args.Length) + "\r\n";
            resp += "$" + cmd.Length + "\r\n" + cmd + "\r\n";
            foreach (var arg in args)
            {
                var argStr = arg.ToString();
                var argStrLength = Encoding.UTF8.GetByteCount(argStr);
                resp += "$" + argStrLength + "\r\n" + argStr + "\r\n";
            }

            var r = Encoding.UTF8.GetBytes(resp);
            try
            {
                Log("C", resp);
                _socket.Send(r);
            }
            catch (SocketException)
            {
                // timeout;
                _socket.Close();
                _socket = null;

                return false;
            }
            return true;
        }

        private async Task<bool> SendCommandAsync(string cmd, params object[] args)
        {
            if (_socket == null)
                await ConnectAsync();
            if (_socket == null)
                return false;

            var resp = "*" + (1 + args.Length) + "\r\n";
            resp += "$" + cmd.Length + "\r\n" + cmd + "\r\n";
            foreach (var arg in args)
            {
                var argStr = arg.ToString();
                var argStrLength = Encoding.UTF8.GetByteCount(argStr);
                resp += "$" + argStrLength + "\r\n" + argStr + "\r\n";
            }

            var r = Encoding.UTF8.GetBytes(resp);
            try
            {
                Log("C", resp);
                await _socket.SendAsync(new ArraySegment<byte>(r), SocketFlags.None);
            }
            catch (SocketException)
            {
                // timeout;
                _socket.Close();
                _socket = null;

                return false;
            }
            return true;
        }

        [Conditional("DEBUG")]
        private void Log(string id, string message)
        {
            Console.WriteLine(id + ": " + message.Trim().Replace("\r\n", " "));
        }

        private void ExpectSuccess()
        {
            var c = _bstream.ReadByte();
            if (c == -1)
                throw new ResponseException("No more data");

            var s = ReadLine();
            Log("S", (char)c + s);
            if (c == '-')
                throw new ResponseException(s.StartsWith("ERR ") ? s.Substring(4) : s);
        }

        private async Task ExpectSuccessAsync(CancellationToken cancellationToken)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(1);
            var c = await _bstream.ReadAsync(buffer, 0, 1, cancellationToken);
            if (c == -1)
                throw new ResponseException("No more data");

            var s = await ReadLineAsync(cancellationToken);
            Log("S", (char)c + s);
            if (c == '-')
                throw new ResponseException(s.StartsWith("ERR ") ? s.Substring(4) : s);
        }

        private void SendExpectSuccess(string cmd, params object[] args)
        {
            if (!SendCommand(cmd, args))
                throw new Exception("Unable to connect");

            ExpectSuccess();
        }

        private async Task SendExpectSuccessAsync(string cmd, params object[] args)
        {
            if (!await SendCommandAsync(cmd, args))
                throw new Exception("Unable to connect");

            ExpectSuccess();
        }

        private int SendDataExpectInt(byte[] data, string cmd, params object[] args)
        {
            if (!SendDataCommand(data, cmd, args))
                throw new Exception("Unable to connect");

            var c = _bstream.ReadByte();
            if (c == -1)
                throw new ResponseException("No more data");

            var s = ReadLine();
            Log("S", (char)c + s);
            if (c == '-')
                throw new ResponseException(s.StartsWith("ERR ") ? s.Substring(4) : s);
            if (c == ':')
            {
                if (int.TryParse(s, out var i))
                    return i;
            }
            throw new ResponseException("Unknown reply on integer request: " + c + s);
        }

        private int SendExpectInt(string cmd, params object[] args)
        {
            if (!SendCommand(cmd, args))
                throw new Exception("Unable to connect");

            var c = _bstream.ReadByte();
            if (c == -1)
                throw new ResponseException("No more data");

            var s = ReadLine();
            Log("S", (char)c + s);
            if (c == '-')
                throw new ResponseException(s.StartsWith("ERR ") ? s.Substring(4) : s);
            if (c == ':')
            {
                if (int.TryParse(s, out var i))
                    return i;
            }
            throw new ResponseException("Unknown reply on integer request: " + c + s);
        }

        private string SendExpectString(string cmd, params object[] args)
        {
            if (!SendCommand(cmd, args))
                throw new Exception("Unable to connect");

            var c = _bstream.ReadByte();
            if (c == -1)
                throw new ResponseException("No more data");

            var s = ReadLine();
            Log("S", (char)c + s);
            if (c == '-')
                throw new ResponseException(s.StartsWith("ERR ") ? s.Substring(4) : s);
            if (c == '+')
                return s;

            throw new ResponseException("Unknown reply on integer request: " + c + s);
        }

        //
        // This one does not throw errors
        //
        private string SendGetString(string cmd, params object[] args)
        {
            if (!SendCommand(cmd, args))
                throw new Exception("Unable to connect");

            return ReadLine();
        }

        private byte[] SendExpectData(string cmd, params object[] args)
        {
            if (!SendCommand(cmd, args))
                throw new Exception("Unable to connect");

            return ReadData();
        }

        private byte[] ReadData()
        {
            var s = ReadLine();
            Log("S", s);
            if (s.Length == 0) throw new ResponseException("Zero length respose");

            var c = s[0];
            if (c == '-') throw new ResponseException(s.StartsWith("-ERR ") ? s.Substring(5) : s.Substring(1));

            if (c == '$')
            {
                if (s == "$-1")
                    return null;

                if (int.TryParse(s.Substring(1), out var n))
                {
                    var retbuf = new byte[n];

                    var bytesRead = 0;
                    do
                    {
                        var read = _bstream.Read(retbuf, bytesRead, n - bytesRead);
                        if (read < 1)
                            throw new ResponseException("Invalid termination mid stream");
                        bytesRead += read;
                    }
                    while (bytesRead < n);

                    if (_bstream.ReadByte() != '\r' || _bstream.ReadByte() != '\n')
                        throw new ResponseException("Invalid termination");
                    return retbuf;
                }
                throw new ResponseException("Invalid length");
            }

            /* don't treat arrays here because only one element works -- use DataArray!
		//returns the number of matches
		if (c == '*') {
			int n;
			if (Int32.TryParse(s.Substring(1), out n)) 
				return n <= 0 ? new byte [0] : ReadData();
			
			throw new ResponseException ("Unexpected length parameter" + r);
		}
		*/

            throw new ResponseException("Unexpected reply: " + s);

        }

        private async Task<byte[]> ReadDataAsync(CancellationToken cancellationToken)
        {
            var s = await ReadLineAsync(cancellationToken);
            Log("S", s);
            if (s.Length == 0) throw new ResponseException("Zero length respose");

            var c = s[0];
            if (c == '-') throw new ResponseException(s.StartsWith("-ERR ") ? s.Substring(5) : s.Substring(1));

            if (c == '$')
            {
                if (s == "$-1")
                    return null;

                if (int.TryParse(s.Substring(1), out var n))
                {
                    var retbuf = new byte[n];

                    var bytesRead = 0;
                    do
                    {
                        var read = await _bstream.ReadAsync(retbuf, bytesRead, n - bytesRead, cancellationToken);
                        if (read < 1)
                            throw new ResponseException("Invalid termination mid stream");
                        bytesRead += read;
                    }
                    while (bytesRead < n);


                    if (await ReadByteAsync(cancellationToken) != '\r' ||
                        await ReadByteAsync(cancellationToken) != '\n')
                        throw new ResponseException("Invalid termination");
                    return retbuf;

                }
                throw new ResponseException("Invalid length");
            }

            /* don't treat arrays here because only one element works -- use DataArray!
		//returns the number of matches
		if (c == '*') {
			int n;
			if (Int32.TryParse(s.Substring(1), out n)) 
				return n <= 0 ? new byte [0] : ReadData();
			
			throw new ResponseException ("Unexpected length parameter" + r);
		}
		*/

            throw new ResponseException("Unexpected reply: " + s);
        }

        public bool ContainsKey(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return SendExpectInt("EXISTS", key) == 1;
        }

        public bool Remove(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return SendExpectInt("DEL", key) == 1;
        }

        public int Remove(params string[] args)
        {
            if (args == null)
                throw new ArgumentNullException(nameof(args));
            return SendExpectInt("DEL", args);
        }

        public int Increment(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return SendExpectInt("INCR", key);
        }

        public int Increment(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return SendExpectInt("INCRBY", key, count);
        }

        public int Decrement(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return SendExpectInt("DECR", key);
        }

        public int Decrement(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return SendExpectInt("DECRBY", key, count);
        }

        public KeyType TypeOf(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            switch (SendExpectString("TYPE", key))
            {
                case "none":
                    return KeyType.None;
                case "string":
                    return KeyType.String;
                case "set":
                    return KeyType.Set;
                case "list":
                    return KeyType.List;
            }
            throw new ResponseException("Invalid value");
        }

        public string RandomKey()
        {
            return SendExpectString("RANDOMKEY");
        }

        public bool Rename(string oldKeyname, string newKeyname)
        {
            if (oldKeyname == null)
                throw new ArgumentNullException(nameof(oldKeyname));
            if (newKeyname == null)
                throw new ArgumentNullException(nameof(newKeyname));
            return SendGetString("RENAME", oldKeyname, newKeyname)[0] == '+';
        }

        public bool Expire(string key, int seconds)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return SendExpectInt("EXPIRE", key, seconds) == 1;
        }

        public bool ExpireAt(string key, int time)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return SendExpectInt("EXPIREAT", key, time) == 1;
        }

        public int TimeToLive(string key)
        {
            if (key == null)
                throw new ArgumentNullException(nameof(key));
            return SendExpectInt("TTL", key);
        }

        public int DbSize => SendExpectInt("DBSIZE");

        public void Save()
        {
            SendExpectSuccess("SAVE");
        }

        public void BackgroundSave()
        {
            SendExpectSuccess("BGSAVE");
        }

        public void Shutdown()
        {
            SendCommand("SHUTDOWN");
            try
            {
                // the server may return an error
                var s = ReadLine();
                Log("S", s);
                if (s.Length == 0)
                    throw new ResponseException("Zero length respose");
                throw new ResponseException(s.StartsWith("-ERR ") ? s.Substring(5) : s.Substring(1));
            }
            catch (IOException)
            {
                // this is the expected good result
                _socket.Close();
                _socket = null;
            }
        }

        public void FlushAll()
        {
            SendExpectSuccess("FLUSHALL");
        }

        public void FlushDb()
        {
            SendExpectSuccess("FLUSHDB");
        }

        private const long UnixEpoch = 621355968000000000L;

        public DateTime LastSave
        {
            get
            {
                var t = SendExpectInt("LASTSAVE");

                return new DateTime(UnixEpoch) + TimeSpan.FromSeconds(t);
            }
        }

        public Dictionary<string, string> GetInfo()
        {
            var r = SendExpectData("INFO");
            var dict = new Dictionary<string, string>();

            foreach (var line in Encoding.UTF8.GetString(r).Split('\n'))
            {
                var p = line.IndexOf(':');
                if (p == -1)
                    continue;
                dict.Add(line.Substring(0, p), line.Substring(p + 1));
            }
            return dict;
        }

        public string[] Keys => GetKeys("*");

        public string[] GetKeys(string pattern)
        {
            if (pattern == null)
                throw new ArgumentNullException(nameof(pattern));

            return SendExpectStringArray("KEYS", pattern);
        }

        public byte[][] MGet(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException(nameof(keys));
            if (keys.Length == 0)
                throw new ArgumentException("keys");

            return SendExpectDataArray("MGET", keys);
        }


        public string[] SendExpectStringArray(string cmd, params object[] args)
        {
            var reply = SendExpectDataArray(cmd, args);
            var keys = new string[reply.Length];
            for (var i = 0; i < reply.Length; i++)
                keys[i] = Encoding.UTF8.GetString(reply[i]);
            return keys;
        }

        public byte[][] SendExpectDataArray(string cmd, params object[] args)
        {
            if (!SendCommand(cmd, args))
                throw new Exception("Unable to connect");
            var c = _bstream.ReadByte();
            if (c == -1)
                throw new ResponseException("No more data");

            var s = ReadLine();
            Log("S", (char)c + s);
            if (c == '-')
                throw new ResponseException(s.StartsWith("ERR ") ? s.Substring(4) : s);
            if (c == '*')
            {
                if (int.TryParse(s, out var count))
                {
                    byte[][] result = null;

                    if (count >= 0)
                    {
                        result = new byte[count][];

                        for (var i = 0; i < count; i++)
                            result[i] = ReadData();
                    }

                    return result;
                }
            }
            throw new ResponseException("Unknown reply on multi-request: " + c + s);
        }

        public async Task<byte[][]> SendExpectDataArrayAsync(string cmd, CancellationToken cancellationToken, params object[] args)
        {
            if (!await SendCommandAsync(cmd, args))
                throw new Exception("Unable to connect");

            var buffer = _arrayPool.Rent(1);
            try
            {
                if (await _bstream.ReadAsync(buffer, 0, 1, cancellationToken) == -1)
                {
                    throw new ResponseException("No more data");
                }

                var c = buffer[0];

                var s = await ReadLineAsync(cancellationToken);
                Log("S", (char) c + s);
                if (c == '-')
                    throw new ResponseException(s.StartsWith("ERR ") ? s.Substring(4) : s);
                if (c == '*')
                {
                    if (int.TryParse(s, out var count))
                    {
                        byte[][] result = null;

                        if (count >= 0)
                        {
                            result = new byte[count][];

                            for (var i = 0; i < count; i++)
                                result[i] = await ReadDataAsync(cancellationToken);
                        }

                        return result;
                    }
                }
                throw new ResponseException("Unknown reply on multi-request: " + c + s);
            }
            finally
            {
                _arrayPool.Return(buffer);
            }
        }

        #region List commands
        public byte[][] ListRange(string key, int start, int end)
        {
            return SendExpectDataArray("LRANGE", key, start, end);
        }

        public void LeftPush(string key, string value)
        {
            LeftPush(key, Encoding.UTF8.GetBytes(value));
        }

        public Task LeftPushAsync(string key, string value, CancellationToken cancellationToken = default(CancellationToken))
        {
            return LeftPushAsync(key, Encoding.UTF8.GetBytes(value), cancellationToken);
        }

        public void LeftPush(string key, byte[] value)
        {
            SendDataCommand(value, "LPUSH", key);
            ExpectSuccess();
        }

        public async Task LeftPushAsync(string key, byte[] value, CancellationToken cancellationToken = default(CancellationToken))
        {
            await SendDataCommandAsync(value, "LPUSH", key);
            await ExpectSuccessAsync(cancellationToken);
        }

        public void RightPush(string key, string value)
        {
            RightPush(key, Encoding.UTF8.GetBytes(value));
        }

        public Task RightPushAsync(string key, string value, CancellationToken cancellationToken = default(CancellationToken))
        {
            return RightPushAsync(key, Encoding.UTF8.GetBytes(value), cancellationToken);
        }

        public void RightPush(string key, byte[] value)
        {
            SendDataCommand(value, "RPUSH", key);
            ExpectSuccess();
        }

        public async Task RightPushAsync(string key, byte[] value, CancellationToken cancellationToken = default(CancellationToken))
        {
            await SendDataCommandAsync(value, "RPUSH", key);
            await ExpectSuccessAsync(cancellationToken);
        }

        public int ListLength(string key)
        {
            return SendExpectInt("LLEN", key);
        }

        public byte[] ListIndex(string key, int index)
        {
            SendCommand("LINDEX", key, index);
            return ReadData();
        }

        public byte[] LeftPop(string key)
        {
            SendCommand("LPOP", key);
            return ReadData();
        }

        public byte[][] BlockingLeftPop(string key, int timeout = 0)
        {
            return SendExpectDataArray("BLPOP", key, timeout);
        }

        public Task<byte[][]> BlockingLeftPopAsync(string key, int timeout = 0, CancellationToken cancellationToken = default(CancellationToken))
        {
            return SendExpectDataArrayAsync("BLPOP", cancellationToken, key, timeout);
        }

        public byte[] RightPop(string key)
        {
            SendCommand("RPOP", key);
            return ReadData();
        }
        public byte[][] BlockingRightPop(string key, int timeout = 0)
        {
            return SendExpectDataArray("BRPOP", key, timeout);
        }

        public Task<byte[][]> BlockingRightPopAsync(string key, int timeout = 0, CancellationToken cancellationToken = default(CancellationToken))
        {
            return SendExpectDataArrayAsync("BRPOP", cancellationToken, key, timeout);
        }

        #endregion

        #region Set commands
        public bool AddToSet(string key, byte[] member)
        {
            return SendDataExpectInt(member, "SADD", key) > 0;
        }

        public bool AddToSet(string key, string member)
        {
            return AddToSet(key, Encoding.UTF8.GetBytes(member));
        }

        public int CardinalityOfSet(string key)
        {
            return SendExpectInt("SCARD", key);
        }

        public bool IsMemberOfSet(string key, byte[] member)
        {
            return SendDataExpectInt(member, "SISMEMBER", key) > 0;
        }

        public bool IsMemberOfSet(string key, string member)
        {
            return IsMemberOfSet(key, Encoding.UTF8.GetBytes(member));
        }

        public byte[][] GetMembersOfSet(string key)
        {
            return SendExpectDataArray("SMEMBERS", key);
        }

        public byte[] GetRandomMemberOfSet(string key)
        {
            return SendExpectData("SRANDMEMBER", key);
        }

        public byte[] PopRandomMemberOfSet(string key)
        {
            return SendExpectData("SPOP", key);
        }

        public bool RemoveFromSet(string key, byte[] member)
        {
            return SendDataExpectInt(member, "SREM", key) > 0;
        }

        public bool RemoveFromSet(string key, string member)
        {
            return RemoveFromSet(key, Encoding.UTF8.GetBytes(member));
        }

        public byte[][] GetUnionOfSets(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException();

            return SendExpectDataArray("SUNION", keys);

        }

        private void StoreSetCommands(string cmd, params string[] keys)
        {
            if (string.IsNullOrEmpty(cmd))
                throw new ArgumentNullException(nameof(cmd));

            if (keys == null)
                throw new ArgumentNullException(nameof(keys));

            SendExpectSuccess(cmd, keys);
        }

        public void StoreUnionOfSets(params string[] keys)
        {
            StoreSetCommands("SUNIONSTORE", keys);
        }

        public byte[][] GetIntersectionOfSets(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException();

            return SendExpectDataArray("SINTER", keys);
        }

        public void StoreIntersectionOfSets(params string[] keys)
        {
            StoreSetCommands("SINTERSTORE", keys);
        }

        public byte[][] GetDifferenceOfSets(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException();

            return SendExpectDataArray("SDIFF", keys);
        }

        public void StoreDifferenceOfSets(params string[] keys)
        {
            StoreSetCommands("SDIFFSTORE", keys);
        }

        public bool MoveMemberToSet(string srcKey, string destKey, byte[] member)
        {
            return SendDataExpectInt(member, "SMOVE", srcKey, destKey) > 0;
        }
        #endregion

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~Redis()
        {
            Dispose(false);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                SendCommand("QUIT");
                ExpectSuccess();
                _socket.Close();
                _socket = null;
                _bstream?.Dispose();
                _streamReader?.Dispose();
            }
        }

        private async Task<int> ReadByteAsync(CancellationToken cancellationToken)
        {
            var buffer = _arrayPool.Rent(1);
            try
            {
                if (await _bstream.ReadAsync(buffer, 0, 1, cancellationToken) == -1) return -1;
                return buffer[0];
            }
            finally 
            {
                _arrayPool.Return(buffer);
            }
        }
    }

    public class SortOptions
    {
        public string Key { get; set; }
        public bool Descending { get; set; }
        public bool Lexographically { get; set; }
        public int LowerLimit { get; set; }
        public int UpperLimit { get; set; }
        public string By { get; set; }
        public string StoreInKey { get; set; }
        public string Get { get; set; }

        public object[] ToArgs()
        {
            var args = new ArrayList();

            if (LowerLimit != 0 || UpperLimit != 0)
            {
                args.Add("LIMIT");
                args.Add(LowerLimit);
                args.Add(UpperLimit);
            }
            if (Lexographically)
                args.Add("ALPHA");
            if (!string.IsNullOrEmpty(By))
            {
                args.Add("BY");
                args.Add(By);
            }
            if (!string.IsNullOrEmpty(Get))
            {
                args.Add("GET");
                args.Add(Get);
            }
            return args.ToArray();
        }
    }

}




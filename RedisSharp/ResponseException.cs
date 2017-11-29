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

namespace RedisSharp
{
    public class ResponseException : Exception
    {
        public ResponseException(string code) : base("Response error")
        {
            Code = code;
        }

        public string Code { get; }
    }
}


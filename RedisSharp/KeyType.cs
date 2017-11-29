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



namespace RedisSharp
{
    public enum KeyType
    {
        None,
        String,
        List,
        Set
    }
}


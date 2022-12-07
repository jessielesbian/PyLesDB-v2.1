import asyncio;
import json;
import websockets;
import asyncio;
import random;
import string;

#I absolutely hate python, but gotta give python addicts a taste of this drug

def get_random_string():
    # choose from all lowercase letter
    letters = string.ascii_letters
    result_str = ''.join(random.choice(letters) for i in range(64))
    return result_str;

class connection:
    def __init__(self, url : str):
        self.__open = True;
        self.__queue = asyncio.Queue();
        self.__pending = dict();
        self.__closetsk = asyncio.Future()
        asyncio.get_event_loop().create_task(self.__event_loop(url))
    async def __send_event_loop(self, websocket):
        while True:
            result = await self.__queue.get();
            if result == "close":
                self.__open = False;
                await websocket.close()
                raise websockets.ConnectionClosedOK(1000, '');
            await websocket.send(result);
    def __exchange(self, key):
        temp = self.__pending.get(key);
        if temp == None:
            return None;
        self.__pending.pop(key);
        return temp;

    async def close(self):
        await self.__queue.put("close");
        await self.__closetsk;

    async def __receive_event_loop(self, websocket):
        while True:
            serialized = await websocket.recv()
            message = json.loads(serialized);
            temp = self.__exchange(message.get("id"));
            if not temp == None:
                temp.set_result(message.get("result"));

    async def __event_loop(self, url):
        async for websocket in websockets.connect(url, subprotocols = ["LesbianDB-v2.1"]):
            try:
                await asyncio.gather(self.__send_event_loop(websocket), self.__receive_event_loop(websocket));
            except websockets.ConnectionClosed as e:
                for item in self.__pending.items():
                    item.set_exception(e)
                if self.__open:
                    continue
                self.__closetsk.set_result(None);
                return;

    async def execute(self, _reads, _conditions, _writes):
        if self.__open:
            randstring = get_random_string()
            encoded = json.dumps(dict(reads = _reads, conditions = _conditions, writes = _writes, id = randstring));

            temp = asyncio.Future()
            self.__pending[randstring] = temp;
            await self.__queue.put(encoded);
            if self.__open:
                return await temp;
        raise websockets.ConnectionClosedOK(1000, '');

class transaction:
    def __init__(self, _connection : connection):
        self.__connection = _connection;
        self.__emptydict = dict()
        self.__optimisticReadCache = dict();
        self.__optimisticWriteCache = dict();
    def write(self, key : str, value : str):
        self.__optimisticWriteCache[key] = value;

    async def read(self, key : str):
        if key in self.__optimisticWriteCache:
            return self.__optimisticWriteCache.get(key);
        if key in self.__optimisticReadCache:
            return self.__optimisticReadCache.get(key);
        temp = (await self.__connection.execute([key], self.__emptydict, self.__emptydict))[key]
        if key in self.__optimisticReadCache:
            return self.__optimisticReadCache.get(key);
        self.__optimisticReadCache[key] = temp;
        return temp;

    async def try_commit(self):
        conditions = self.__optimisticReadCache;
        if(len(self.__optimisticWriteCache) == 0):
            conditions = dict()
        keys = []
        for key in self.__optimisticReadCache.keys():
            keys.append(key)
        result = await self.__connection.execute(keys, conditions, self.__optimisticWriteCache)
        self.__optimisticWriteCache.clear();
        self.__optimisticReadCache = result;
        for kvp in result.items():
            if result.get(kvp[0]) != kvp[1]:
                return False;
        return True;
    async def try_volatile_read(self, keys):
        result = await self.__connection.execute(keys, self.__emptydict, self.__emptydict)
        filtered = dict()
        for kvp in result.items():
            if kvp[0] in self.__optimisticReadCache:
                if kvp[1] != self.__optimisticReadCache.get(kvp[0]):
                    self.__optimisticWriteCache.clear()
                    self.__optimisticReadCache = result
                    return (False, None);
            else:
                self.__optimisticReadCache[kvp[0]] = kvp[1]
            if kvp[0] in self.__optimisticWriteCache:
                filtered[kvp[0]] = self.__optimisticWriteCache.get(kvp[0]);
            else:
                filtered[kvp[0]] = kvp[1]
        return (True, filtered)

import PyLesDB;
import asyncio;


async def main():
    transaction = PyLesDB.transaction(PyLesDB.connection("ws://localhost:1234/"))
    val = None;
    val2 = None;
    while True:
        #Increment counter 1
        val = await transaction.read("PyLesDB.Counter1");
        if val == None:
            val = "0"
        transaction.write("PyLesDB.Counter1", str(int(val) + 1))

        #Increment counter 2
        ctr2 = await transaction.try_volatile_read(["PyLesDB.Counter2"]);
        if not ctr2[0]:
            #revert and restart
            continue
        val2 = ctr2[1]["PyLesDB.Counter2"]
        if val2 == None:
            val2 = "0"
        transaction.write("PyLesDB.Counter2", str(int(val2) + 1))

        if(await transaction.try_commit()):
            break

    print("counter 1: " + val)
    print("counter 2: " + val2)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()


import asyncio

class NumStream:
    def __init__(self):
        self._n = 0

    def __aiter__(self):                 # ① 返回迭代器（通常返回 self）
        return self

    async def __anext__(self):           # ② 每次取一个元素（协程）
        if self._n >= 3:
            raise StopAsyncIteration     # ③ 结束信号
        self._n += 1
        await asyncio.sleep(0.1)         # 模拟异步 IO（可挂起）
        return self._n

async def main():
    async for x in NumStream():
        print(x)

asyncio.run(main())
# 输出：1 2 3

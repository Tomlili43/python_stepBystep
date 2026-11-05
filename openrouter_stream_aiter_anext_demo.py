from collections.abc import AsyncIterator, Awaitable, Callable
# 导入 asyncio，用于 wait_for 实现流空闲超时（对应原版 resolve_stream_idle_timeout_s）
import asyncio
# 导入 sys，用于在无 key 时退出程序
import sys
# only  API_KEY necessary
# ======== 1. 你只需要修改下面这几个常量 ========
API_KEY = ""            # OpenRouter 的 API key
MODEL = "z-ai/glm-5.2"                      # 想用的模型（OpenRouter 格式：厂商/模型名）
PROMPT = "用一句话介绍你自己"                       # 发给模型的一句话
IDLE_TIMEOUT_S = 30.0                             # 流空闲超时：超过这个秒数没有任何新 chunk 就报错

# ======== 2. 简化版的“流式回调”签名（模拟 nanobot 的 on_content_delta） ========
# 类型别名：一个 async 回调，接收一段增量文本 str，返回 None
ContentDeltaCb = Callable[[str], Awaitable[None]]


async def print_delta(text: str) -> None:
    # 定义一个最简单的回调：把增量文本直接打印，不换行，并立即刷新终端缓冲
    print(text, end="", flush=True)


async def chat_stream(
    messages: list[dict],                                  # OpenAI 格式的对话消息列表
    on_content_delta: ContentDeltaCb | None = None,        # 每收到一段正文增量就调用的回调
) -> str:
    # 惰性导入 openai SDK（模拟原版 _ensure_client 的“用到现在才 import”，节省启动时间）
    from openai import AsyncOpenAI

    # 创建异步客户端：base_url 指向 OpenRouter 的 OpenAI 兼容端点
    client = AsyncOpenAI(
        api_key=API_KEY,                                   # 认证用的密钥
        base_url="https://openrouter.ai/api/v1",           # OpenRouter 兼容 OpenAI 协议的地址
    )

    # 发起流式请求，返回值是一个“异步流”对象（对应原版 client.chat.completions.create(**kwargs) 且 stream=True）
    stream = await client.chat.completions.create(
        model=MODEL,                                       # 指定模型
        messages=messages,                                 # 对话上下文
        stream=True,                                       # 关键：开启流式返回
        stream_options={"include_usage": True},            # 让最后一个 chunk 带 token 用量（同原版）
    )

    # 用内置函数 aiter() 拿到异步迭代器（等价于调用 stream.__aiter__()，这是原版的写法）
    stream_iter: AsyncIterator = aiter(stream)

    # 收集所有 chunk，最后一次性解析（对应原版 chunks: list[Any] = []）
    chunks = []
    # 是否收到过 finish_reason（原版用它判断“流是否正常结束”）
    completed = False

    # 死循环 + anext() 逐块拉取：这是本示例的核心，等价于原版的 while True / __anext__()
    while True:
        try:
            # anext() 取下一个 chunk；用 wait_for 包一层实现“空闲超时”（同原版 asyncio.wait_for(stream_iter.__anext__(), ...)）
            chunk = await asyncio.wait_for(anext(stream_iter), timeout=IDLE_TIMEOUT_S)
        except StopAsyncIteration:                         # 异步迭代器耗尽时抛出这个异常
            break                                          # 正常退出循环
        except TimeoutError:                               # 等待超过 IDLE_TIMEOUT_S 仍未有新数据
            raise ConnectionError(f"流空闲超过 {IDLE_TIMEOUT_S} 秒，已中断") from None  # 模拟原版的超时报错

        # 把 chunk 存起来，供最后统计用量（对应原版 chunks.append(chunk)）
        chunks.append(chunk)

        # 取该 chunk 的第一个 choice（OpenAI 流式协议里每次增量都包在 choices[0].delta 里）
        choice = chunk.choices[0] if chunk.choices else None
        if choice is None:                                 # 最后一个 usage chunk 没有 choices，跳过
            continue
        if choice.finish_reason:                           # finish_reason 非空说明模型输出结束
            completed = True                               # 标记流正常收尾（对应原版 completed |= bool(...)）
        delta = choice.delta                               # delta 才是本次的“增量”内容
        if on_content_delta and delta.content:             # 有回调且本块带正文增量
            await on_content_delta(delta.content)          # 实时把增量交给调用方（如逐字打印）

    # 流结束但从未收到 finish_reason，说明连接被提前掐断（同原版 ConnectionError）
    if not completed:
        raise ConnectionError("流在收到 finish_reason 之前就结束了")

    # 从收集的 chunks 里提取 token 用量（最后一个 chunk 的 usage 字段，简化版只取总数）
    usage = getattr(chunks[-1], "usage", None)             # 没有则得到 None
    if usage:                                              # 用量存在时打印
        print(f"\n[token 用量] 输入 {usage.prompt_tokens}，输出 {usage.completion_tokens}")

    # 返回完整回复文本（简化版不再做原版 _parse_chunks 的工具调用聚合）
    return "".join(c.choices[0].delta.content or "" for c in chunks if c.choices)


async def main() -> None:
    # 简单的 key 校验，避免把占位符直接发出去
    if "你的openrouter密钥" in API_KEY or not API_KEY.strip():
        print("请先在 main.py 顶部填入你的 OpenRouter API key")  # 提示用户
        sys.exit(1)                                        # 以错误码退出

    # 组装 OpenAI 格式的消息列表：一条用户消息即可
    messages = [{"role": "user", "content": PROMPT}]       # role=content 结构与原版 _ALLOWED_MSG_KEYS 一致

    # 调用流式函数，传入 print_delta 作为回调，实现“打字机”效果
    full = await chat_stream(messages, on_content_delta=print_delta)

    # 最后打印分隔线和完整文本，方便对照
    print("\n--- 完整回复 ---")                             # 分隔提示
    print(full)                                            # 完整聚合结果


if __name__ == "__main__":
    # asyncio.run 启动事件循环并运行 main 协程
    asyncio.run(main())

"""ARC オンライン対戦の中継サーバー（WebSocket）。

同じ部屋番号を入れた 2 人をつなぎ、片方から届いたメッセージをもう片方へそのまま転送するだけ。
ゲームの中身は見ない（対戦は両者のゲームが、共有した乱数の種と送り合う操作から同じ結果を出す）。
対戦中に通信が切れても、部屋の席を GRACE 秒残しておき、同じ合言葉（token）でつなぎ直せば続きから遊べる。

起動:  py server/relay.py [--port 8765]
環境変数 PORT があればそちらを使う（クラウドに置くとき）。
必要なもの: Python 3.9 以上と websockets（pip install websockets）

メッセージ（JSON。"t" が種類）:
  クライアント → サーバー
    {"t": "join", "room": "1234", "ver": "<ゲームのバージョン>", "token": "<合言葉>"}   部屋に入る（同じ token ならつなぎ直し）
    {"t": "relay", ...}                                              相手にそのまま転送する
    {"t": "leave"}                                                   部屋を出る（相手には peer_left）
  サーバー → クライアント
    {"t": "wait"}                    相手を待っている（先に入った側）
    {"t": "matched", "side": 0|1}    2 人そろった（先に入った側が 0）
    {"t": "rejoined", "side": 0|1}   つなぎ直せた
    {"t": "peer_lost", "grace": 60}  相手の通信が切れた（grace 秒は席を残す）
    {"t": "peer_back"}               相手がつなぎ直した
    {"t": "peer_left"}               相手が部屋を出た・時間内に戻らなかった
    {"t": "error", "msg": "..."}     部屋が満員・バージョン違い・部屋番号の形が違う
    {"t": "relay", ...}              相手から届いたもの
"""

import argparse
import asyncio
import json
import os
import re

import websockets

ROOM_RE = re.compile(r"^[0-9]{4,8}$")
GRACE = 60  # 切れた人の席を残す秒数
rooms: dict = {}  # 部屋番号 -> {"ver": str, "slots": [{"ws", "token", "timer"}, ...]}（最大 2 席）


async def send(ws, msg: dict) -> None:
    if ws is None:
        return
    try:
        await ws.send(json.dumps(msg, ensure_ascii=False))
    except websockets.ConnectionClosed:
        pass


def other(room: dict, slot: dict):
    for s in room["slots"]:
        if s is not slot:
            return s
    return None


async def drop_slot(code: str, slot: dict) -> None:
    """席をなくして、相手に peer_left を知らせる（部屋はなくなる）"""
    room = rooms.get(code)
    if room is None or slot not in room["slots"]:
        return
    o = other(room, slot)
    del rooms[code]
    if o is not None:
        if o["timer"] is not None:
            o["timer"].cancel()
        await send(o["ws"], {"t": "peer_left"})


async def grace_timer(code: str, slot: dict) -> None:
    try:
        await asyncio.sleep(GRACE)
    except asyncio.CancelledError:
        return
    if slot["ws"] is None:
        await drop_slot(code, slot)


async def join(ws, msg: dict):
    """部屋に入る。入れたら (部屋番号, 席) を返す"""
    code = str(msg.get("room", ""))
    ver = str(msg.get("ver", ""))
    token = str(msg.get("token", ""))[:64]
    if not ROOM_RE.match(code):
        await send(ws, {"t": "error", "msg": "部屋番号は 4〜8 桁の数字にしてください"})
        return None
    room = rooms.get(code)
    if room is None:
        slot = {"ws": ws, "token": token, "timer": None}
        rooms[code] = {"ver": ver, "slots": [slot]}
        await send(ws, {"t": "wait"})
        return code, slot
    # つなぎ直し（同じ合言葉の席がある）
    for i, s in enumerate(room["slots"]):
        if token and s["token"] == token:
            old = s["ws"]
            s["ws"] = ws
            if s["timer"] is not None:
                s["timer"].cancel()
                s["timer"] = None
            if old is not None and old is not ws:
                await old.close()
            if len(room["slots"]) < 2:
                await send(ws, {"t": "wait"})
            else:
                await send(ws, {"t": "rejoined", "side": i})
                await send(other(room, s)["ws"], {"t": "peer_back"})
            return code, s
    if len(room["slots"]) >= 2:
        await send(ws, {"t": "error", "msg": "この部屋はもう 2 人そろっています"})
        return None
    if room["ver"] != ver:
        text = "相手とゲームのバージョンが違います（両方とも最新版にしてください）"
        await send(ws, {"t": "error", "msg": text})
        await send(room["slots"][0]["ws"], {"t": "error", "msg": text})
        return None
    slot = {"ws": ws, "token": token, "timer": None}
    room["slots"].append(slot)
    for i, s in enumerate(room["slots"]):
        await send(s["ws"], {"t": "matched", "side": i})
    return code, slot


async def handler(ws) -> None:
    code = None
    slot = None
    try:
        async for raw in ws:
            if len(raw) > 65536:
                continue
            try:
                msg = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(msg, dict):
                continue
            t = msg.get("t")
            if t == "join" and slot is None:
                r = await join(ws, msg)
                if r is not None:
                    code, slot = r
            elif t == "relay" and slot is not None and code in rooms:
                o = other(rooms[code], slot)
                if o is not None:
                    await send(o["ws"], msg)  # 相手が切れている間は捨てる（つなぎ直したときに、クライアントが送り直す）
            elif t == "leave" and slot is not None:
                await drop_slot(code, slot)
                slot = None
    except websockets.ConnectionClosed:
        pass
    finally:
        if slot is not None and code in rooms and slot["ws"] is ws:
            room = rooms[code]
            if len(room["slots"]) < 2:
                del rooms[code]  # まだ相手が来ていない部屋は、すぐなくす
            else:
                slot["ws"] = None
                o = other(room, slot)
                if o["ws"] is None:
                    # 両方とも切れた: 先に切れた方のタイマーに任せる。どちらも戻らなければ部屋はなくなる
                    pass
                await send(o["ws"], {"t": "peer_lost", "grace": GRACE})
                slot["timer"] = asyncio.ensure_future(grace_timer(code, slot))


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8765")))
    ap.add_argument("--host", default=None)  # 省略時は IPv4・IPv6 の両方で待ち受ける（localhost が ::1 になる環境でもつながるように）
    args = ap.parse_args()
    async with websockets.serve(handler, args.host, args.port, max_size=1 << 20, ping_interval=20, ping_timeout=40):
        print(f"ARC relay server: port {args.port}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())

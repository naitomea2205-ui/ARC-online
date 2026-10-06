"""ARC オンライン対戦の中継サーバー（WebSocket）。

同じ部屋番号を入れた 2 人をつなぎ、片方から届いたメッセージをもう片方へそのまま転送するだけ。
ゲームの中身は見ない（対戦は両者のゲームが、共有した乱数の種と送り合う操作から同じ結果を出す）。

起動:  py server/relay.py [--port 8765]
環境変数 PORT があればそちらを使う（クラウドに置くとき）。
必要なもの: Python 3.9 以上と websockets（pip install websockets）

メッセージ（JSON。"t" が種類）:
  クライアント → サーバー
    {"t": "join", "room": "1234", "ver": "<ゲームのバージョン>"}   部屋に入る
    {"t": "relay", ...}                                              相手にそのまま転送する
  サーバー → クライアント
    {"t": "wait"}                    相手を待っている（先に入った側）
    {"t": "matched", "side": 0|1}    2 人そろった（先に入った側が 0）
    {"t": "error", "msg": "..."}     部屋が満員・バージョン違い・部屋番号の形が違う
    {"t": "peer_left"}               相手が切断した
    {"t": "relay", ...}              相手から届いたもの
"""

import argparse
import asyncio
import json
import os
import re

import websockets

ROOM_RE = re.compile(r"^[0-9]{4,8}$")
rooms: dict = {}  # 部屋番号 -> [ws, ...]（最大 2）
versions: dict = {}  # ws -> ver


async def send(ws, msg: dict) -> None:
    try:
        await ws.send(json.dumps(msg, ensure_ascii=False))
    except websockets.ConnectionClosed:
        pass


async def handler(ws) -> None:
    room = None
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
            if t == "join" and room is None:
                code = str(msg.get("room", ""))
                if not ROOM_RE.match(code):
                    await send(ws, {"t": "error", "msg": "部屋番号は 4〜8 桁の数字にしてください"})
                    continue
                members = rooms.setdefault(code, [])
                if len(members) >= 2:
                    await send(ws, {"t": "error", "msg": "この部屋はもう 2 人そろっています"})
                    continue
                ver = str(msg.get("ver", ""))
                if members and versions.get(members[0]) != ver:
                    await send(ws, {"t": "error", "msg": "相手とゲームのバージョンが違います（両方とも最新版にしてください）"})
                    await send(members[0], {"t": "error", "msg": "相手とゲームのバージョンが違います（両方とも最新版にしてください）"})
                    continue
                room = code
                versions[ws] = ver
                members.append(ws)
                if len(members) == 1:
                    await send(ws, {"t": "wait"})
                else:
                    for i, m in enumerate(members):
                        await send(m, {"t": "matched", "side": i})
            elif t == "relay" and room is not None:
                for m in rooms.get(room, []):
                    if m is not ws:
                        await send(m, msg)
    except websockets.ConnectionClosed:
        pass
    finally:
        versions.pop(ws, None)
        if room is not None and room in rooms:
            members = rooms[room]
            if ws in members:
                members.remove(ws)
            for m in members:
                await send(m, {"t": "peer_left"})
            if not members:
                del rooms[room]


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

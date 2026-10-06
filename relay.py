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
    {"t": "join", "room": "1234", "ver": "...", "spectate": true}      対戦中の部屋を観戦する
    {"t": "spec_req"}                                               観戦者: 対戦の記録をもう一度頼む（届かないとき）
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
    {"t": "relay", ...}              相手から届いたもの（対戦者の relay は観戦者にも届く。"to" があればその観戦者にだけ）
  観戦
    {"t": "spectating"}              観戦者: 観戦を始めた（対戦者から対戦の記録 spec_state が届く）
    {"t": "spec_request", "id": n}   対戦者: 観戦者 n に対戦の記録を送ってほしい（relay で "to": n を付けて送る）
    {"t": "spec_count", "n": n}      対戦者: 観戦者の人数
    {"t": "room_closed"}             観戦者: 対戦者が部屋を出た
"""

import argparse
import asyncio
import json
import os
import re

import websockets

ROOM_RE = re.compile(r"^[0-9]{4,8}$")
GRACE = 60  # 切れた人の席を残す秒数
rooms: dict = {}  # 部屋番号 -> {"ver": str, "slots": [{"ws", "token", "timer"}, ...]（最大 2 席）, "specs": {id: ws}（観戦者）}
MAX_SPECS = 20
_spec_seq = 0


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


async def to_specs(room: dict, msg: dict) -> None:
    for w in list(room.get("specs", {}).values()):
        await send(w, msg)


async def spec_count(room: dict) -> None:
    for s in room["slots"]:
        await send(s["ws"], {"t": "spec_count", "n": len(room.get("specs", {}))})


async def ask_state(room: dict, sid: int) -> None:
    """観戦者 sid のために、つながっている対戦者に対戦の記録を頼む"""
    for s in room["slots"]:
        if s["ws"] is not None:
            await send(s["ws"], {"t": "spec_request", "id": sid})
            return


async def drop_slot(code: str, slot: dict) -> None:
    """席をなくして、相手に peer_left を知らせる（部屋はなくなる）"""
    room = rooms.get(code)
    if room is None or slot not in room["slots"]:
        return
    o = other(room, slot)
    del rooms[code]
    await to_specs(room, {"t": "room_closed"})
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
    if msg.get("spectate"):
        global _spec_seq
        if room is None:
            await send(ws, {"t": "error", "msg": "その部屋番号の部屋はありません"})
            return None
        if len(room["slots"]) < 2:
            await send(ws, {"t": "error", "msg": "まだ対戦が始まっていません（2 人そろってから観戦できます）"})
            return None
        if room["ver"] != ver:
            await send(ws, {"t": "error", "msg": "対戦者とゲームのバージョンが違います（最新版にしてください）"})
            return None
        if len(room.setdefault("specs", {})) >= MAX_SPECS:
            await send(ws, {"t": "error", "msg": "観戦できる人数がいっぱいです"})
            return None
        _spec_seq += 1
        sid = _spec_seq
        room["specs"][sid] = ws
        await send(ws, {"t": "spectating", "id": sid})
        await spec_count(room)
        await ask_state(room, sid)
        return code, {"spec": sid}
    if room is None:
        slot = {"ws": ws, "token": token, "timer": None}
        rooms[code] = {"ver": ver, "slots": [slot], "specs": {}}
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
                await to_specs(room, {"t": "peer_back"})
                await spec_count(room)
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
            if len(raw) > 262144:
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
            elif t == "spec_req" and slot is not None and "spec" in slot and code in rooms:
                await ask_state(rooms[code], slot["spec"])
            elif t == "relay" and slot is not None and "spec" not in slot and code in rooms:
                room = rooms[code]
                to = msg.get("to")
                if to is not None:
                    await send(room.get("specs", {}).get(int(to)), msg)  # 観戦者 1 人への対戦の記録
                    continue
                o = other(room, slot)
                if o is not None:
                    await send(o["ws"], msg)  # 相手が切れている間は捨てる（つなぎ直したときに、クライアントが送り直す）
                await to_specs(room, msg)  # 観戦者にも同じ操作を届ける
            elif t == "leave" and slot is not None and "spec" not in slot:
                await drop_slot(code, slot)
                slot = None
    except websockets.ConnectionClosed:
        pass
    finally:
        if slot is not None and "spec" in slot:
            room = rooms.get(code)
            if room is not None and room.get("specs", {}).pop(slot["spec"], None) is not None:
                await spec_count(room)
            slot = None
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
                await to_specs(room, {"t": "peer_lost", "grace": GRACE})
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

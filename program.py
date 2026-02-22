# program.py
# -*- coding: utf-8 -*-

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from telethon import TelegramClient
from telethon.errors import FloodWaitError, RPCError

API_ID = 37930540
API_HASH = "d94a6e7d6ccc9f931e93db1f3097b079"
SESSION_NAME = "Meoxman"

# ТАРГЕТЫ (60 групп): можно ссылки, @username, или numeric id -100...
TARGETS: List[Union[str, int]] = [
    @CITY_yuk_markazi197519702
    @yuk_xizmati1
    @Logistikagroup
    @yuk_gruppa
    @transeuazia
    @YUKMARKAZI_Yukbor_Fura
    @Kazsng
    @Logistika_super_UZB
    @gruzoperevozki_rossiya
    @worldlogistic
    @yuk_markazi_gruppasi
    @logisticscargo
    @pitak_guripasi_yuk_markazi
    @Transportationuz
    @rossiya_gruzoperivozka
    @OOLIONGROUP
    @milyukkar
    @trucking1
    @LOGISTIKA_24
    @Logistics_com
    @logistika_uz_ru_tr_kz_global_cng
    @mycargo
    @Yukla24_uzb
    @logisticscargoo
    @Yuk_markazi_yuk_yukla
    @yuk_bor_uzz
    @pitak_markazi
    @rossia1_2
    @Yuk_markazi_isuzu_uz
    @globalfreightexchange
    @gruz032
    @fayz_logistic
    @DUNYO_BOYLAB_YUKLAR
    @yukbor7
    @LOGISTIKA_UZBEKISTANA
    @XALQARO_YUKLARI
    @gruzoperevozki_rossia
    @dalneboyuzsng
    @shafiyorlar
    @UZ_RUS_XALQARO_YUKLARI
    @VODIY_BOYLAB_YUKLAR
    @yukmarkazi2
    @gruzoperevozmir
    @uzbekistonboylabyuklar
    @yuk_gruppa1
    @mirovoy_gruz01
    @akum_tashkent
    @mejdunarodni_yukgurpa
    @ileritranscomrussia
    @yukboruzb
    @Russian_logistics
    @intercargouzz
    @perevozkinss
    @yuktawiw
    @CargoAuto
    @yuk_uz_24
    @logistic_ravonkarvon_gruz
    @Yuk_logistika_markazi
    @yukmarkazi_furalar
    @S_N_G_RUS_GRUZ
]

@dataclass
class RateLimits:
    # Пауза между отправками В ОДНУ группу (уменьшает риск флуда)
    delay_between_sends_sec: float = 0.8

    # Сколько сообщений максимум отправлять подряд, потом "подышать"
    burst_size: int = 20
    cooldown_sec: float = 8.0

    # Отправка группами: сколько таргетов обрабатывать за один проход
    batch_targets: int = 10
    batch_pause_sec: float = 3.0

LIMITS = RateLimits(
    delay_between_sends_sec=0.8,
    burst_size=20,
    cooldown_sec=8.0,
    batch_targets=10,
    batch_pause_sec=3.0,
)

# ==========================
# 2) TELETHON CLIENT
# ==========================
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

async def ensure_login():
    if not client.is_connected():
        await client.connect()
    if not await client.is_user_authorized():
        print("\n[TELEGRAM] First run: sign-in required.")
        await client.start()  # спросит телефон/код в консоли
        print("[TELEGRAM] Authorized OK.")

# ==========================
# 3) ОЧЕРЕДЬ ОТПРАВКИ
# ==========================

class Job(BaseModel):
    text: str
    targets: Optional[List[Union[str, int]]] = None  # если None -> TARGETS
    meta: Optional[Dict[str, Any]] = None

queue: "asyncio.Queue[Job]" = asyncio.Queue()
worker_task: Optional[asyncio.Task] = None

async def resolve_entity(t: Union[str, int]):
    return await client.get_entity(t)

async def send_job(job: Job) -> Dict[str, Any]:
    text = job.text.strip()
    targets = job.targets or TARGETS
    if not targets:
        raise ValueError("No targets configured.")

    report = {"ok": [], "failed": [], "total": len(targets), "started_at": int(time.time())}

    sent_in_a_row = 0

    # бьём таргеты на батчи
    for batch_start in range(0, len(targets), LIMITS.batch_targets):
        batch = targets[batch_start: batch_start + LIMITS.batch_targets]

        for idx, t in enumerate(batch, start=batch_start + 1):
            try:
                ent = await resolve_entity(t)
                await client.send_message(ent, text)
                report["ok"].append({"target": t, "index": idx})
                sent_in_a_row += 1

                # маленькая пауза после каждого отправления
                await asyncio.sleep(LIMITS.delay_between_sends_sec)

                # "burst" лимит — чтобы регулярно делать паузы
                if sent_in_a_row >= LIMITS.burst_size:
                    await asyncio.sleep(LIMITS.cooldown_sec)
                    sent_in_a_row = 0

            except FloodWaitError as e:
                wait_s = int(getattr(e, "seconds", 0) or 0)
                report["failed"].append({"target": t, "index": idx, "error": f"FLOOD_WAIT_{wait_s}s"})
                # лучше реально подождать — иначе Telegram будет только хуже
                if wait_s > 0:
                    await asyncio.sleep(wait_s + 1)

            except RPCError as e:
                report["failed"].append({"target": t, "index": idx, "error": f"RPCError: {type(e).__name__}: {e}"})

            except Exception as e:
                report["failed"].append({"target": t, "index": idx, "error": f"{type(e).__name__}: {e}"})

        # пауза между батчами
        if batch_start + LIMITS.batch_targets < len(targets):
            await asyncio.sleep(LIMITS.batch_pause_sec)

    report["finished_at"] = int(time.time())
    return report

async def worker_loop():
    print("[worker] started")
    while True:
        job = await queue.get()
        try:
            rep = await send_job(job)
            print(f"[worker] job done: ok={len(rep['ok'])} failed={len(rep['failed'])}")
        except Exception as e:
            print(f"[worker] job failed: {type(e).__name__}: {e}")
        finally:
            queue.task_done()

# ==========================
# 4) FASTAPI (опционально)
# ==========================
app = FastAPI(title="telegram_sender")

@app.on_event("startup")
async def on_startup():
    global worker_task
    await ensure_login()
    worker_task = asyncio.create_task(worker_loop())
    print("[app] ready")

@app.on_event("shutdown")
async def on_shutdown():
    if worker_task:
        worker_task.cancel()
    try:
        await client.disconnect()
    except Exception:
        pass

@app.get("/health")
async def health():
    return {"status": "ok", "targets": len(TARGETS)}

@app.post("/enqueue")
async def enqueue(job: Job):
    await queue.put(job)
    return {"status": "queued", "queue_size": queue.qsize()}

# ==========================
# 5) STANDALONE РЕЖИМ
# ==========================
# Запуск без сервера:
# python program.py
async def standalone_demo():
    await ensure_login()
    demo_text = "Тестовая рассылка: проверка доставки."
    rep = await send_job(Job(text=demo_text))
    print(rep)

if __name__ == "__main__":
    # Standalone: отправит сразу во все TARGETS
    asyncio.run(standalone_demo())

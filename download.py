#!/usr/bin/env python3
import os
import asyncio
import httpx
import time

from datetime import datetime
from twitchdl.utils import format_size
from dataclasses import dataclass
from typing import Dict, Optional

from prompt_toolkit import Application, HTML
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout.containers import HSplit, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.layout.layout import Layout
from prompt_toolkit.layout import Dimension


# WORKER_POOL_SIZE = 5
CHUNK_SIZE = 1024 * 256
# CONNECT_TIMEOUT = 5
# RETRY_COUNT = 5


@dataclass
class Task:
    id: int
    pid: int
    filename: str
    size: Optional[int] = None
    downloaded: int = 0
    progress: int = 0

    def report_chunk(self, chunk_size):
        self.downloaded += chunk_size
        self.progress = int(100 * self.downloaded / self.size)


class Downloader:
    start_time: float
    avg_speed: float
    chunk_count: int
    bytes_downloaded: int = 0
    vod_count: int = 0
    vod_size: int = 0
    estimated_total: int
    progress: int = 0

    active_tasks: Dict[int, Task] = {}

    def __init__(self, worker_count: int):
        self.start_time = time.time()
        self.worker_count = worker_count
        self.progress_bars = [
            Window(height=1, content=FormattedTextControl(text=HTML("<gray>Worker idle</gray>")))
            for _ in range(worker_count)
        ]

        kb = KeyBindings()
        kb.add("q")(lambda event: event.app.exit())

        header_content = FormattedTextControl(text=HTML("<gray>Initializing</gray>"))
        header = Window(height=1, content=header_content)
        divider = Window(height=1, char='-')
        root_container = HSplit([header, divider] + self.progress_bars)
        layout = Layout(root_container)

        self.app = Application(layout=layout, key_bindings=kb)

    async def run(self, loop, sources, targets):
        if len(sources) != len(targets):
            raise ValueError(f"Got {len(sources)} sources but {len(targets)} targets.")

        self.chunk_count = len(sources)
        loop.create_task(self.download(sources, targets))
        await self.app.run_async()

    async def download(self, sources, targets):
        await download_all(sources, targets, self.worker_count,
            self.on_init, self.on_start, self.on_progress, self.on_end)
        self.app.exit()

    def on_init(self, task_id: int, filename: str):
        # Occupy an unoccupied progress bar
        pid = self.get_free_pid()
        task = Task(task_id, pid, filename)
        self.active_tasks[task_id] = task
        self.set_progress(pid, f"{task.filename} | Initializing...")

    def get_free_pid(self):
        occupied_pids = [t.pid for t in self.active_tasks.values()]
        pid = next(pid for pid in range(self.worker_count) if pid not in occupied_pids)
        return pid

    def on_start(self, task_id: int, size: int):
        task = self.active_tasks[task_id]
        task.size = size

        self.vod_count += 1
        self.vod_size += size
        self.estimated_total = int(self.chunk_count * self.vod_size / self.vod_count)
        self.progress = int(100 * self.bytes_downloaded / self.estimated_total)

    def on_progress(self, task_id: int, chunk_size: int):
        task = self.active_tasks[task_id]
        task.report_chunk(chunk_size)
        self.set_progress(task.pid, f"{task.filename} | {task.progress}%")

    def on_end(self, task_id: int):
        task = self.active_tasks[task_id]
        self.set_progress(task.pid, "<gray>Idle</gray>")
        del self.active_tasks[task_id]

    def set_progress(self, pid, message):
        self.progress_bars[pid].content.text = HTML(f"#{pid:02} | {message}")
        self.app.invalidate()


async def download_one(client, semaphore, task_id, source, target, on_init, on_start, on_progress, on_end):
    async with semaphore:
        with open(target, 'wb') as f:
            # TODO: handle failure (retries etc)
            on_init(task_id, os.path.basename(target))
            async with client.stream('GET', source) as response:
                size = int(response.headers.get('content-length'))
                on_start(task_id, size)
                async for chunk in response.aiter_bytes(chunk_size=CHUNK_SIZE):
                    f.write(chunk)
                    on_progress(task_id, len(chunk))
                on_end(task_id)


async def download_all(sources, targets, workers, on_init, on_start, on_progress, on_end):
    async with httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(workers)
        tasks = [download_one(client, semaphore, task_id, source, target, on_init, on_start, on_progress, on_end)
                 for task_id, (source, target) in enumerate(zip(sources, targets))]
        await asyncio.gather(*tasks)

urls = [
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/0.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/1.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/2.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/3.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/4.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/5.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/6.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/7.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/8.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/9.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/10.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/11.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/12.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/13.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/14.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/15.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/16.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/17.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/18.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/19.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/20.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/21.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/22.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/23.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/24.ts",
    "https://dgeft87wbj63p.cloudfront.net/5ea498c795c4c4b9d1e1_bananasaurus_rex_43776026428_1636653613/chunked/25.ts",
]

targets = [os.path.basename(url).zfill(8) for url in urls]
zipped = list(zip(urls, targets))

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    d = Downloader(15)
    loop.run_until_complete(d.run(loop, urls, targets))

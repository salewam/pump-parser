"""
Parse task management with dedup and auto-cleanup.
Thread-safe, persists to JSON file.
"""
import json
import os
import time
import uuid
import threading


class TaskManager:
    """Manages parse tasks with dedup by filename+size."""

    def __init__(self, tasks_file=None):
        if tasks_file is None:
            from config import TASKS_FILE
            tasks_file = tasks_file or TASKS_FILE
        self._file = tasks_file
        self._lock = threading.Lock()
        self._tasks = self._load()

    def _load(self):
        if os.path.exists(self._file):
            try:
                with open(self._file) as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save(self):
        """Save only completed/error tasks (not in-progress — those are in memory only)."""
        os.makedirs(os.path.dirname(self._file), exist_ok=True)
        to_save = {
            tid: t for tid, t in self._tasks.items()
            if t.get("status") in ("done", "error")
        }
        with open(self._file, "w") as f:
            json.dump(to_save, f, ensure_ascii=False)

    def create_task(self, filename, file_size):
        """Create or return existing task. Returns (task_id, is_new)."""
        with self._lock:
            # Dedup: same filename + same size = same catalog
            for tid, t in self._tasks.items():
                if (t.get("filename") == filename
                        and t.get("file_size") == file_size
                        and t.get("status") == "done"):
                    return tid, False

            task_id = uuid.uuid4().hex[:12]
            self._tasks[task_id] = {
                "status": "pending",
                "filename": filename,
                "file_size": file_size,
                "start_time": time.time(),
                "progress": 0,
            }
            return task_id, True

    def get_task(self, task_id):
        """Get task by ID. Returns dict or None."""
        with self._lock:
            return self._tasks.get(task_id)

    def update_task(self, task_id, data):
        """Update task fields."""
        with self._lock:
            if task_id in self._tasks:
                self._tasks[task_id].update(data)
                if data.get("status") in ("done", "error"):
                    self._save()

    def delete_task(self, task_id):
        """Delete a task."""
        with self._lock:
            self._tasks.pop(task_id, None)
            self._save()

    def list_tasks(self, status=None):
        """List all tasks, optionally filtered by status."""
        with self._lock:
            if status:
                return {tid: t for tid, t in self._tasks.items() if t.get("status") == status}
            return dict(self._tasks)

    def cleanup_old(self, days=30):
        """Remove tasks older than N days."""
        cutoff = time.time() - days * 86400
        removed = 0
        with self._lock:
            to_remove = [
                tid for tid, t in self._tasks.items()
                if t.get("start_time", 0) < cutoff
            ]
            for tid in to_remove:
                del self._tasks[tid]
                removed += 1
            if removed:
                self._save()
        return removed

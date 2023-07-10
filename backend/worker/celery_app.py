import os
from celery import Celery

celery_app = Celery(
    "celery_worker",
    broker=os.getenv("CELERY_BROKER_URI"),
    backend=os.getenv("CELERY_BACKEND_URI"),
)

celery_app.conf.update(result_extended=True)

celery_app.conf.timezone = "UTC"
celery_app.conf.enable_utc = True
celery_app.autodiscover_tasks(["worker.tasks"])

if __name__ == "__main__":
    # Test task execution:
    # from app.worker.tasks import ..
    # ...delay(..)
    pass

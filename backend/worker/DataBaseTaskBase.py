from celery import Task

from app.custom_logging.get_logger import get_logger
from app.db.util import get_db_session

logger = get_logger(__name__)


class DatabaseTask(Task):
    db_session = None

    def before_start(self, task_id, args, kwargs):
        self.db_session = get_db_session()

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        self.session.commit()
        self.session.close()
        self.db_session = None

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self.session.commit()
        self.session.close()
        self.db_session = None

    def retry(self, args=None, kwargs=None, exc=None, throw=True,
              eta=None, countdown=None, max_retries=None, **options):
        self.session.commit()
        self.session.close()
        self.db_session = None
        super().retry(args=args, kwargs=kwargs, exc=exc, throw=throw, eta=eta, countdown=countdown,
                      max_retries=max_retries, **options)

    @property
    def session(self):
        if not self.db_session:
            self.db_session = get_db_session()
        return self.db_session

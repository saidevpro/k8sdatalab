from apscheduler.schedulers.background import BackgroundScheduler

from .notifications import run_due_notifications


def start_scheduler(app):
    scheduler = BackgroundScheduler(timezone="Europe/Paris")

    def job():
        with app.app_context():
            run_due_notifications()

    scheduler.add_job(job, "cron", minute="*", id="daily_notifications", replace_existing=True)
    scheduler.start()
    return scheduler

# Import smtplib for the actual sending function
import smtplib
from datetime import datetime, timedelta
from typing import Dict

from origamibot import OrigamiBot as Bot

# Import the email modules we'll need
from email.message import EmailMessage

import sqlalchemy
import yaml
from sqlalchemy.orm import sessionmaker, Session

import models
from main import get_db_uri


def send_notification(content: str, config: Dict, session: Session = None):
    message = EmailMessage()
    message.set_content(content)

    bot = Bot(config["token"])
    bot.send_message(config["chat_id"], content)

    if session:
        record = models.Notification(datetime.now())
        session.add(record)
        session.commit()

    return


def measuremnts_too_old(session: Session, station_id: int):
    dt = datetime.now()

    cutoff = dt - timedelta(hours=2)

    if (
        session.query(models.TemperatureMeasurement)
        .filter(models.TemperatureMeasurement.timestamp > cutoff)
        .filter(models.TemperatureMeasurement.station_id == station_id)
        .count()
        == 0
    ):
        return True
    if (
        session.query(models.HumidityMeasurement)
        .filter(models.HumidityMeasurement.timestamp > cutoff)
        .filter(models.HumidityMeasurement.station_id == station_id)
        .count()
        == 0
    ):
        return True
    return False


def msg_due(session: Session):
    dt = datetime.now()
    cutoff = dt - timedelta(days=1)
    return session.query(models.Notification.timestamp > cutoff).count() == 0


def monitor():
    engine = sqlalchemy.create_engine(get_db_uri())
    Session = sessionmaker(bind=engine)
    session = Session()
    config = yaml.safe_load(open("conf/monitor.yaml"))
    try:
        if measuremnts_too_old(session, config["station_id"]) and msg_due(session):
            send_notification("There seems to be data missing", config, session=session)
    except Exception as e:
        send_notification(f"Could not corry or something {str(e)}", config)


if __name__ == "__main__":
    monitor()

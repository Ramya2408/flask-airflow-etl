from configparser import ConfigParser

def get_db_config():
    cp = ConfigParser()
    cp.read('/home/ramya/Ramya_project/flask-airflow-etl/config/config.ini')
    return dict(cp['postgres'])

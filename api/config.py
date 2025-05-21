from configparser import ConfigParser

def get_db_config():
    cp = ConfigParser()
    cp.read('../config/config.ini')
    return dict(cp['postgres'])

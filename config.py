db_asset = {
    "host": "192.168.88.254",
    "port": 3306,
    "user": "root",
    "passwd": "Mofang123",
    "db": "asset_allocation",
    "charset": "utf8"
}

db_base = {
    "host": "192.168.88.254",
    "port": 3306,
    "user": "root",
    "passwd": "Mofang123",
    "db": "mofang",
    "charset": "utf8"
}

db_wind = {
    "host": "192.168.88.11",
    "port": 3306,
    "user": "public",
    "passwd": "h76zyeTfVqAehr5J",
    "db": "wind",
    "charset": "utf8"
}


db_asset_uri = "mysql+pymysql://root:Mofang123@192.168.88.254/asset_allocation?charset=utf8&use_unicode=1"
db_base_uri = "mysql+pymysql://root:Mofang123@192.168.88.254/mofang?charset=utf8&use_unicode=1"
db_wind_uri = "mysql+pymysql://public:h76zyeTfVqAehr5J@192.168.88.11/wind?charset=utf8&use_unicode=1"

uris = {
    'asset_allocation': db_asset_uri,
    'mofang': db_base_uri,
    'wind': db_wind_uri
}
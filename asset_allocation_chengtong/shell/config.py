#coding=utf8

db_asset = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": "Mofang123",
    "db":"asset_allocation",
    "charset": "utf8"
}

db_base = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": "Mofang123",
    "db":"mofang",
    "charset": "utf8"
}



db_caihui = {
    "host": "db_caihui.licaimofang.com",
    "port": 3306,
    "user": "finance",
    "passwd": "lk8sge9jcdhw",
    "db":"caihui",
    "charset": "utf8"
}



db_portfolio_sta = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": "Mofang123",
    "db":"portfolio_statistics",
    "charset": "utf8"
}

db_wind = {
    "host": "db_asset.licaimofang.com",
    "port": 3306,
    "user": "finance",
    "passwd": "lk8sge9jcdhw",
    "db":"lcmf_wind",
    "charset": "utf8"
    }



db_asset_uri = 'mysql://root:Mofang123@127.0.0.1/asset_allocation?charset=utf8&use_unicode=1'
#db_base_uri =  'mysql://finance:lk8sge9jcdhw@db_base.licaimofang.com/mofang_api?charset=utf8&use_unicode=1'
db_base_uri =  'mysql://root:Mofang123@127.0.0.1/mofang?charset=utf8&use_unicode=1'
db_caihui_uri =  'mysql://finance:lk8sge9jcdhw@db_caihui.licaimofang.com/caihui?charset=utf8&use_unicode=1'
db_portfolio_sta_uri = 'mysql://root:Mofang123@127.0.0.1/portfolio_statistics?charset=utf8&use_unicode=1'
db_mapi_uri =  'mysql://root:Mofang123@localhost/mapi?charset=utf8&use_unicode=1'
db_trade_uri =  'mysql://root:Mofang123@127.0.0.1/trade?charset=utf8&use_unicode=1'
db_tongji_uri =  'mysql://root:Mofang123@127.0.0.1/tongji?charset=utf8&use_unicode=1'
db_factor_uri =  'mysql://root:Mofang123@127.0.0.1/multi_factor?charset=utf8&use_unicode=1'
db_wind_uri = 'mysql://finance:lk8sge9jcdhw@db_asset.licaimofang.com/lcmf_wind?charset=utf8&use_unicode=1'
db_windsync_uri = 'mysql://root:Mofang123@127.0.0.1/wind_filesync?charset=utf8&use_unicode=1'


uris = {
    'asset': db_asset_uri,
    'base': db_base_uri,
    'caihui': db_caihui_uri,
    'portfolio_sta': db_portfolio_sta_uri,
    #'portfolio_sta': config.db_portfolio_sta_uri,
    'mapi': db_mapi_uri,
    'trade': db_trade_uri,
    'tongji': db_tongji_uri,
    'wind': db_wind_uri,
    'windsync': db_windsync_uri,
    'factor':db_factor_uri,
}

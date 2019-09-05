#/bin/bash
mysqldump -h192.168.88.254 -uroot -pMofang123 mofang trade_dates > trade_dates.sql
mysql -h192.168.88.254 -uroot -pMofang123 asset_allocation < trade_dates.sql

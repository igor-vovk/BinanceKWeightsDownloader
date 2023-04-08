# Toolkit to download Binance trading history data and insert it into SQL-database

This is by no means a ready-made project. I haven't tested it 
and it's not ready to use out of the box.
The project was written in a day to download some historic trading
data and insert it into MySQL database.

If you still want to try running it, there is `init.sql` file in the `resources` folder with the database structure.
Database access as well as trading pairs to download are configured in `application.conf` file.

If you just want to download trade data, there are read-made scripts here: https://github.com/binance/binance-public-data
, I used them as a source.

## TODO
* Store archive checksums in the database and use them for verification 
Convert influx fields to tags by manipulating line protocol.

WARNING: This is still in a very alpha-stage. Don't run on critical system without testing first!

# Instalation
```sh
go get -u github.com/rvolosatovs/influx-taggify
```
# Usage
```sh
influx_inspect export -database "$db" -datadir "$datadir" -waldir "$waldir" -out /tmp/influx-export
# Delete the measurements you don't need to convert using `sed`/`perl` (i.e. `perl -in -e 'print unless m/^unrelated_measurement.*/' /tmp/influx-export`)
influx-taggify -in /tmp/influx-export -out /tmp/influx-export-tagged fieldFoo fieldBar
# Drop the old database or edit generated file to change the name of the database
influx -import -path /tmp/influx-export-tagged
```

It worked for my use case, but your mileage may vary.
Try locally on non-critical setup first!
Feel free to try, report issues and contribute! :)

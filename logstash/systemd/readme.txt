cp routelogbeat.service /lib/systemd/system/

systemctl daemon-reload 2> /dev/null

sudo systemctl enable routelogbeat
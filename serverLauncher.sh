#!/bin/sh

gnome-terminal -e "python3 zz-server.py"
gnome-terminal -e "ldf-server servConfig.json 4000 1"
gnome-terminal -e "node latency_proxy.js http://127.0.0.1:5000 3000 50"
gnome-terminal -e "node latency_proxy.js http://127.0.0.1:4000 2000 50"

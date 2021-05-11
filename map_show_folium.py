#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :main.py
# @Time      :2021/5/5 5:25 下午
# @Author    :Kinddle
import folium
# import pandas as pd
import io
import webbrowser
import socket
import threading
import random


class WebThread(threading.Thread):
    run_flag = True
    PORT = 8080

    def __init__(self, map_data, port):
        threading.Thread.__init__(self)
        self.PORT = port
        self.map_data = map_data
        self.socket_html = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_html.bind(("127.0.0.1", port))

    def stop(self):
        self.run_flag = False
        self.__del__()

    def __del__(self):
        super(threading.Thread)
        self.socket_html.close()
        print(f"connection in port {self.PORT} shutdown")

    def run(self):
        with io.BytesIO() as myio:
            self.map_data.save(myio, False)
            data = myio.getvalue()
        self.socket_html.listen(10)
        print(f"ready for connection in port {self.PORT}")
        while self.run_flag:
            try:
                conn, addr = self.socket_html.accept()
            except ConnectionAbortedError:
                continue
            msg = conn.recv(1024 * 12)
            print(f"connect from <{addr}> with {msg[:20]}...")
            conn.sendall(bytes("HTTP/1.1 201 OK\r\n\r\n", "utf-8"))
            conn.sendall(data)
            conn.close()


def show_map(map_data):
    port = random.randint(8000, 60000)
    x = WebThread(map_data, port)
    x.start()
    webbrowser.open_new(f"http://localhost:{port}")
    #  通过x.stop关闭socket，节省资源
    return x


if __name__ == '__main__':

    #  code from there
    m = folium.Map(location=[39.93, 116.40], zoom_start=10)

    m.add_child(folium.LatLngPopup())

    show_map(m)

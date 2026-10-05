"""Fail closed even when application retry code swallows a connection exception."""
import socket

attempts=[]
_installed=False


def deny(address):
    attempts.append(repr(address))
    raise AssertionError('External network forbidden in tests')


def install():
    global _installed
    if _installed:return
    _installed=True
    original=socket.socket.connect
    original_ex=socket.socket.connect_ex
    def connect(sock,address):
        if sock.family in (socket.AF_INET,socket.AF_INET6):return deny(address)
        return original(sock,address)
    def connect_ex(sock,address):
        if sock.family in (socket.AF_INET,socket.AF_INET6):return deny(address)
        return original_ex(sock,address)
    socket.socket.connect=connect
    socket.socket.connect_ex=connect_ex
    socket.create_connection=lambda address,*args,**kwargs:deny(address)
    socket.getaddrinfo=lambda host,*args,**kwargs:deny(host)

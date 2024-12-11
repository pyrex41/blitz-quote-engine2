import os

class Config(object):
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'you-will-never-guess'
    CSG_TOKEN = os.environ.get('CSG_TOKEN') or None
    API_KEY = os.environ.get('API_KEY') or '2150e5ea35698640582ef9c511c8090210b2f7a0f8e53672094b8e5d3c7f9275'
    #BASIC_AUTH_FORCE = True

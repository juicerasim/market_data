import os
from dotenv import load_dotenv

load_dotenv() 

class Settings:
    key = os.getenv("key", "app")
    secret = os.getenv("secret", "app")
    socketEndpoint = os.getenv("socketEndpoint", "app")


settings = Settings()

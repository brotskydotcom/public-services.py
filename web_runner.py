# Copyright (c) 2019 Daniel C. Brotsky.  All rights reserved.
import uvicorn

from app.services.main import app

if __name__ == '__main__':
    uvicorn.run(app, host="localhost", port=8080)

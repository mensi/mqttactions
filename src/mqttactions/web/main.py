import threading

from uvicorn import Server, Config

from mqttactions.web.app import app, manager


def run(port: int):
    # Register the web manager with the runtime
    from mqttactions.runtime import set_web_manager
    set_web_manager(manager)

    config = Config(app, host="0.0.0.0", port=port, log_level="info")
    server = Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    def shutdown():
        server.should_exit = True
    return shutdown
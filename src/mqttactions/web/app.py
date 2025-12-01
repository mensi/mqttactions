import asyncio

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.templating import Jinja2Templates

from mqttactions.web.websocket import ConnectionManager
from mqttactions.inmemory_client import InMemoryMqttClient
from mqttactions.runtime import get_client, get_subscribed_topics
from mqttactions.statemachine import get_state_machines
from mqttactions.web.models import InitialData, StateMachineData, InjectMessage

# Expect templates in the same directory as this file
templates_path = Path(__file__).parent
templates = Jinja2Templates(directory=str(templates_path))

manager = ConnectionManager()


@asynccontextmanager
async def lifespan(active_app: FastAPI):
    manager.set_loop(asyncio.get_running_loop())
    yield

app = FastAPI(lifespan=lifespan)


def is_test_mode() -> bool:
    try:
        client = get_client()
        return isinstance(client, InMemoryMqttClient)
    except:
        return False


@app.get("/")
async def get_root(request: Request):
    return templates.TemplateResponse("index.html", {
        "request": request,
        "is_test_mode": is_test_mode()
    })


@app.get("/api/data", response_model=InitialData)
async def get_initial_data():
    """Provides the initial state to the web UI."""
    try:
        topics = get_subscribed_topics()

        statemachines_data = []
        for sm in get_state_machines():
            try:
                sm_data = StateMachineData(
                    name=sm.name,
                    currentState=sm.get_current_state_name(),
                    diagram=sm.to_model()
                )
                statemachines_data.append(sm_data)
            except Exception as e:
                # Log error but continue with other state machines
                import logging
                logging.error(f"Error processing state machine {getattr(sm, 'name', 'unknown')}: {e}")

        return InitialData(topics=topics, statemachines=statemachines_data)
    except Exception as e:
        import logging
        logging.error(f"Error in get_initial_data: {e}")
        return InitialData(topics=[], statemachines=[])


@app.get("/simulate")
async def get_simulate(request: Request):
    if not is_test_mode():
        # Redirect to root if not in test mode
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/")
        
    return templates.TemplateResponse("simulate.html", {
        "request": request,
        "is_test_mode": True
    })


@app.post("/api/inject")
async def inject_message(message: InjectMessage):
    if not is_test_mode():
        return {"error": "Not in test mode"}
    
    client = get_client()
    if isinstance(client, InMemoryMqttClient):
        client.inject_message(message.topic, message.payload)
        return {"status": "ok", "message": f"Injected {message.payload} to {message.topic}"}
    
    return {"error": "Client is not InMemoryMqttClient"}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # We just keep the connection open to receive broadcasts.
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

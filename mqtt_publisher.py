import os
import asyncio
from fastapi import FastAPI
import serial_asyncio
from asyncio_mqtt import Client
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

load_dotenv()

# 환경 변수 로드
SERIAL_PORT = os.getenv("SERIAL_PORT", "COM6")
SERIAL_BAUDRATE = int(os.getenv("SERIAL_BAUDRATE", "115200"))
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "admin")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "admin")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "farm/sensors/all")
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", "10485760"))
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))
PUBLISHER_PORT = int(os.getenv("PUBLISHER_PORT", "8000"))

# 로거 설정
if not os.path.exists("logs"):
    os.makedirs("logs")

sensor_logger = logging.getLogger("sensor_logger")
if not sensor_logger.handlers:
    sensor_logger.setLevel(logging.INFO)
    handler = RotatingFileHandler("logs/sensor_raw.log", maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding="utf-8")
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    handler.setFormatter(formatter)
    sensor_logger.addHandler(handler)

app = FastAPI(title="Farmtos MQTT Publisher")

is_active = True

# Serial → MQTT Publisher
async def serial_publisher():
    while True:
        try:
            reader, writer = await serial_asyncio.open_serial_connection(
                url=SERIAL_PORT, baudrate=SERIAL_BAUDRATE
            )
            print(f"{SERIAL_PORT} 접속 성공, 데이터 수신을 시작합니다.")

            async with Client(MQTT_HOST, MQTT_PORT, username=MQTT_USER, password=MQTT_PASSWORD) as client:
                while True:
                    try:
                        line = await reader.readuntil(b'\n')             
                        line = line.decode().strip()

                        if line and is_active:
                            sensor_logger.info(line)
                            await client.publish(MQTT_TOPIC, line, qos=1)

                    except Exception as e:
                        print("시리얼 통신 에러 (케이블 분리 등):", e)
                        break
        
        except Exception as e:
            print(f"{SERIAL_PORT} 포트를 찾을 수 없거나 열 수 없습니다. 3초 후 재시도합니다.", e)
            
        await asyncio.sleep(3)

@app.on_event("startup")
async def startup():
    asyncio.create_task(serial_publisher())

@app.get("/api/status")
async def get_receive_status():
    return {"is_active": is_active}

@app.post("/api/toggle")
async def toggle_receive_status():
    global is_active
    is_active = not is_active
    state_str = "ON" if is_active else "OFF"
    return {"is_active": is_active, "message": f"Publisher 데이터 수집이 {state_str} 되었습니다."}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "publisher", "is_active": is_active}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("mqtt_publisher:app", host="0.0.0.0", port=PUBLISHER_PORT, reload=True)

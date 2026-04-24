import os
import asyncio
from fastapi import FastAPI
import serial_asyncio
from datetime import datetime, timedelta
from asyncio_mqtt import Client
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
import json

load_dotenv()

# 환경 변수 로드
SERIAL_PORT = os.getenv("SERIAL_PORT", "COM7")
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
global_writer = None

# Serial → MQTT Publisher
async def serial_publisher():
    global global_writer
    while True:
        if not is_active:
            await asyncio.sleep(2)
            continue

        try:
            reader, writer = await serial_asyncio.open_serial_connection(
                url=SERIAL_PORT, baudrate=SERIAL_BAUDRATE
            )
            global_writer = writer
            print(f"{SERIAL_PORT} 접속 성공, 데이터 수신을 시작합니다.")
            try:
                async with Client(MQTT_HOST, MQTT_PORT, username=MQTT_USER, password=MQTT_PASSWORD) as client:
                    while is_active:
                        created_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        try:
                            line = await reader.readuntil(b'\n')
                            
                            print(f"시리얼포트 {SERIAL_PORT} | 데이터 길이 : {len(line)}")
                            
                            if not line:
                                break
                            line = line.decode().strip()

                            if line:
                                try:
                                    data_dict = json.loads(line)
                                    for key, val in data_dict.items():
                                        if isinstance(val, dict):
                                            val['created_date_time'] = created_date_time
                                    line = json.dumps(data_dict)
                                except Exception as e:
                                    print("JSON Parse Error:", e)

                                print('line', line)
                                sensor_logger.info(line)
                                await client.publish(MQTT_TOPIC, line, qos=1)

                        except Exception as e:
                            if is_active:
                                print("시리얼 통신 에러 (케이블 분리 등):", e)
                                sensor_logger.error(f"시리얼 통신 에러: {e}")
                            else:
                                print("사용자 요청으로 시리얼 연결을 종료했습니다.")
                            break
            except Exception as e:
                print(f"{MQTT_HOST}:{MQTT_PORT} MQTT 브로커가 실행 중인지 확인하세요.", e)
                sensor_logger.error(f"MQTT 연결 에러: {e}")

        except Exception as e:
            if is_active:
                print(f"{SERIAL_PORT} 포트를 찾을 수 없거나 열 수 없습니다. 3초 후 재시도합니다.", e)
                sensor_logger.error(f"시리얼 포트 열기 에러: {e}")
                
        finally:
            if global_writer:
                try:
                    global_writer.close()
                except Exception:
                    pass
                global_writer = None
            
        await asyncio.sleep(3)

@app.on_event("startup")
async def startup():
    asyncio.create_task(serial_publisher())

@app.get("/api/status")
async def get_receive_status():
    return {"is_active": is_active}

@app.post("/api/toggle")
async def toggle_receive_status():
    global is_active, global_writer
    is_active = not is_active
    state_str = "ON" if is_active else "OFF"
    
    if not is_active:
        if global_writer:
            print("시리얼 포트 연결을 해제합니다.")
            try:
                global_writer.close()
            except Exception:
                pass
            global_writer = None

    return {"is_active": is_active, "message": f"Publisher 데이터 수집이 {state_str} 되었습니다."}

@app.get("/health")
async def health():
    return {"status": "ok", "service": "publisher", "is_active": is_active}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("mqtt_publisher:app", host="0.0.0.0", port=PUBLISHER_PORT, reload=True)

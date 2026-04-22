import os
import asyncio
import json
import sqlite3
import shutil
from datetime import datetime
from fastapi import FastAPI
from asyncio_mqtt import Client
from dotenv import load_dotenv

load_dotenv()

# 환경 변수 로드
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "admin")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "admin")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "farm/sensors/all")
CONSUMER_PORT = int(os.getenv("CONSUMER_PORT", "8001"))
STATS_INTERVAL_MINUTES = int(os.getenv("STATS_INTERVAL_MINUTES", "10"))

app = FastAPI(title="Farmtos MQTT Consumer")

is_active = True
DB_FILE = "base_sensor_raw2.db"

# DB
def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute(''' CREATE TABLE IF NOT EXISTS sensor_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sensor_type TEXT,
        raw_value TEXT,
        measured_value REAL,
        sensor_line TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP )
    ''')
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sensor_type ON sensor_data(sensor_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON sensor_data(created_at)")
    conn.close()

db_buffer = []

def insert_data(data):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for sensor_type, values in data.items():
        print(sensor_type,"|", values)

        try:
            types = values['desc'].split("|")
            pos = 0

            for val in values['value'].split("|"):
                raw_val = values['raw'].split("|")[pos]
                db_buffer.append((f"{types[pos].upper()}", raw_val, val, sensor_type, current_time))
                pos += 1
        except:
            db_buffer.append((values['desc'].upper(), values['raw'], values['value'], sensor_type, current_time))

async def db_writer():
    while True:
        await asyncio.sleep(2)
        if db_buffer:
            items = db_buffer[:]
            db_buffer.clear()
            
            try:
                conn = sqlite3.connect(DB_FILE)
                cur = conn.cursor()
                cur.executemany("""
                INSERT INTO sensor_data (sensor_type, raw_value, measured_value, sensor_line, created_at)
                VALUES (?, ?, ?, ?, ?)
                """, items)
                conn.commit()
                conn.close()
            except Exception as e:
                print("DB Bulk Insert Error:", e)

# MQTT Consumer
async def mqtt_consumer():
    async with Client(MQTT_HOST, MQTT_PORT, username=MQTT_USER, password=MQTT_PASSWORD, client_id="farmtos_main_consumer", clean_session=False) as client:
        async with client.unfiltered_messages() as messages:
            await client.subscribe(MQTT_TOPIC, qos=1)

            async for msg in messages:
                try:
                    payload = msg.payload.decode()
                    data = json.loads(payload)
                                       
                    if is_active:
                        insert_data(data)
                except Exception as e:
                    print("Error mqtt_consumer:", e, "Payload:", payload if 'payload' in locals() else "N/A")

async def stats_routine():
    while True:
        await asyncio.sleep(STATS_INTERVAL_MINUTES * 60)
        try:
            conn = sqlite3.connect(DB_FILE)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("""
                SELECT sensor_line, COUNT(*) as count, MAX(created_at) as last_time
                FROM sensor_data
                GROUP BY sensor_line
            """)
            rows = cur.fetchall()
            conn.close()
            
            stats = [dict(row) for row in rows]
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Periodic Sensor Stats:")
            print(json.dumps(stats, ensure_ascii=False, indent=2))
        except Exception as e:
            print("Stats Routine Error:", e)

@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(db_writer())
    asyncio.create_task(mqtt_consumer())
    asyncio.create_task(stats_routine())

@app.get("/api/stats")
async def get_sensor_stats():
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("""
            SELECT sensor_line, COUNT(*) as count, MAX(created_at) as last_time
            FROM sensor_data
            GROUP BY sensor_line
        """)
        rows = cur.fetchall()
        conn.close()
        return {"status": "ok", "stats": [dict(row) for row in rows]}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/status")
async def get_receive_status():
    return {"is_active": is_active}

@app.post("/api/toggle")
async def toggle_receive_status():
    global is_active
    is_active = not is_active
    state_str = "ON" if is_active else "OFF"
    return {"is_active": is_active, "message": f"Consumer 데이터 기록이 {state_str} 되었습니다."}

@app.get("/health")
async def health():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM sensor_data")
    count = cur.fetchone()[0]
    conn.close()
    
    total, used, free = shutil.disk_usage(".")
    free_gb = round(free / (1024 ** 3), 2)
    
    return {
        "status": "ok", 
        "service": "consumer",
        "row count": count,
        "disk_free_gb": free_gb, 
        "is_active": is_active
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=CONSUMER_PORT, reload=True)

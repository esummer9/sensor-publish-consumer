import os
import asyncio
import json
import sqlite3
import shutil
from datetime import datetime, timedelta
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, FileResponse
from asyncio_mqtt import Client
from dotenv import load_dotenv
import openpyxl

load_dotenv()

# 환경 변수 로드
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "admin")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "admin")
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "farm/sensors/all")
CONSUMER_PORT = int(os.getenv("CONSUMER_PORT", "8001"))
STATS_INTERVAL_MINUTES = int(os.getenv("STATS_INTERVAL_MINUTES", "10"))
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "6698600477")
EXPORT_DAYS = int(os.getenv("EXPORT_DAYS", "2"))

FARM_CODE = os.getenv("FARM_CODE", "K9HLJC")
FARM_ID = os.getenv("FARM_ID", "58")

app = FastAPI(title="Farmtos MQTT Consumer")

is_active = True
DB_FILE = os.getenv("DB_FILE", "base_sensor_raw423.db")
OF_DB_FILE = os.getenv("OF_DB_FILE", "./farmtos_of.db")

# DB
def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute(''' CREATE TABLE IF NOT EXISTS sensor_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sensor_type TEXT,
        raw_value TEXT,
        measured_value REAL,
        sensor_line TEXT,
        collector_id TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP )
    ''')
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sensor_type ON sensor_data(sensor_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON sensor_data(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_sensor_line ON sensor_data(sensor_line)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_collector_id ON sensor_data(collector_id)")
    conn.close()

db_buffer = []

db_of_buffer = []

def insert_data(data):

    for sensor_type, values in data.items():
        # print(sensor_type,"|", values)

        try:
            types = values['desc'].split("|")
            pos = 0

            for val in values['value'].split("|"):
                raw_val = values['raw'].split("|")[pos]
                db_buffer.append((f"{types[pos].upper()}", raw_val, val, sensor_type, values['collector'], values['created_date_time']))
                pos += 1
        except:
            db_buffer.append((values['desc'].upper(), values['raw'], values['value'], sensor_type, values['collector'], values['created_date_time']))

        values['FARM_CODE'] = FARM_CODE
        values['FARM_ID'] = FARM_ID
        values['sensor_type'] = sensor_type 
        values['current_time'] = values['created_date_time'] 
        
        db_of_buffer.append((sensor_type, values))

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
                INSERT INTO sensor_data (sensor_type, raw_value, measured_value, sensor_line, collector_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """, items)
                conn.commit()
                conn.close()
            except Exception as e:
                print("DB Bulk Insert Error:", e)

def insert_solar_data(cur, conn, solar_items, sensor_ids):
    if not solar_items:
        return
        
    solar_insert_data = []
    for item in solar_items:
        farm_device_id = sensor_ids.get(item.get('sensor_type'), 0)
        farm_id = item.get('FARM_ID')
        collect_date = item.get('created_date_time')
        
        # 값 파싱 (단일 값이거나 파이프로 구분된 값일 수 있음)
        val_str = str(item.get('value', None))
        
        solar = val_str if val_str != 'None' else None
        accumulated_solar = None
        photon = None
        
        is_outlier = 0
        is_missing = 0
        
        solar_insert_data.append((
            farm_device_id, farm_id, collect_date, 
            solar, accumulated_solar, photon, is_outlier, is_missing
        ))

    if solar_insert_data:
        try:
            cur.executemany("""
                INSERT INTO tb_farmtos_of_collect_solar 
                (farm_device_id, farm_id, collect_date, solar, accumulated_solar, photon, is_outlier, is_missing) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, solar_insert_data)
            conn.commit()
        except Exception as e:
            print("tb_farmtos_of_collect_solar Insert Error:", e)

def insert_weather_station_data(cur, conn, weather_station_items, sensor_ids):
    if not weather_station_items:
        return
        
    ws_insert_data = []
    for item in weather_station_items:
        farm_device_id = sensor_ids.get(item.get('sensor_type'), 0)
        farm_id = item.get('FARM_ID')
        collect_date = item.get('created_date_time')
        
        val_str = str(item.get('value', None))
        val = float(val_str) if (val_str and val_str != 'None') else None
        
        desc = item.get('desc', '')
        
        outside_co2 = None
        outside_humidity = None
        outside_temperature = None
        outside_wind_direction = None
        outside_wind_speed = None
        
        if 'CO2' in desc:
            outside_co2 = val
        elif 'Wind Speed' in desc:
            outside_wind_speed = val
        elif 'Wind Direction' in desc:
            outside_wind_direction = val
            
        is_outlier = 0
        is_missing = 0
        
        ws_insert_data.append((
            farm_device_id, farm_id, collect_date, 
            outside_co2, outside_humidity, outside_temperature,
            outside_wind_direction, outside_wind_speed,
            is_outlier, is_missing
        ))

    if ws_insert_data:
        try:
            cur.executemany("""
                INSERT INTO tb_farmtos_of_collect_weather_station 
                (farm_device_id, farm_id, collect_date, 
                 outside_co2, outside_humidity, outside_temperature, 
                 outside_wind_direction, outside_wind_speed, 
                 is_outlier, is_missing) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, ws_insert_data)
            conn.commit()
        except Exception as e:
            print("tb_farmtos_of_collect_weather_station Insert Error:", e)

def insert_fdr_data(cur, conn, fdr_items, sensor_ids):
    if not fdr_items:
        return
        
    fdr_insert_data = []
    for item in fdr_items:
        farm_device_id = sensor_ids.get(item.get('sensor_type'), 0)
        farm_id = item.get('FARM_ID')
        collect_date = item.get('created_date_time')
        
        val_str = str(item.get('value', None))
        desc = item.get('desc', '')
        
        soil_temperature_value = None
        soil_humidity_value = None
        soil_ec = None
        
        if '|' in val_str:
            vals = val_str.split('|')
            try:
                soil_humidity_value = float(vals[0]) if len(vals) > 0 and vals[0] and vals[0] != 'None' else None
                soil_temperature_value = float(vals[1]) if len(vals) > 1 and vals[1] and vals[1] != 'None' else None
                soil_ec = float(vals[2]) if len(vals) > 2 and vals[2] and vals[2] != 'None' else None
            except ValueError:
                pass
        else:
            val = None
            try:
                val = float(val_str) if (val_str and val_str != 'None') else None
            except ValueError:
                pass
                
            desc_upper = desc.upper()
            if 'TEMP' in desc_upper or '온도' in desc_upper:
                soil_temperature_value = val
            elif 'HUMI' in desc_upper or '습도' in desc_upper:
                soil_humidity_value = val
            elif 'EC' in desc_upper:
                soil_ec = val
        
        is_outlier = 0
        is_missing = 0
        
        sensor_origin = {'temperature': soil_temperature_value, 'humidity': soil_humidity_value, 'ec': soil_ec}
        fdr_insert_data.append((
            farm_device_id, farm_id, collect_date, 
            soil_temperature_value, soil_humidity_value, soil_ec,
            f'{sensor_origin}', is_outlier, is_missing
        ))
        
    if fdr_insert_data:
        try:
            cur.executemany("""
                INSERT INTO tb_farmtos_of_collect_fdr 
                (farm_device_id, farm_id, collect_date, 
                 soil_temperature_value, soil_humidity_value, soil_ec, 
                 sensor_origin, is_outlier, is_missing) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, fdr_insert_data)
            conn.commit()
            
            # tb_farm_device와 조인하여 위치 정보(latitude, longitude, width_x, height_y) 업데이트 (후처리)
            cur.execute("""
                UPDATE tb_farmtos_of_collect_fdr
                SET 
                    latitude = (SELECT latitude FROM tb_farm_device WHERE tb_farm_device.farm_device_id = tb_farmtos_of_collect_fdr.farm_device_id),
                    longitude = (SELECT longitude FROM tb_farm_device WHERE tb_farm_device.farm_device_id = tb_farmtos_of_collect_fdr.farm_device_id),
                    width_x = (SELECT width_x FROM tb_farm_device WHERE tb_farm_device.farm_device_id = tb_farmtos_of_collect_fdr.farm_device_id),
                    height_y = (SELECT height_y FROM tb_farm_device WHERE tb_farm_device.farm_device_id = tb_farmtos_of_collect_fdr.farm_device_id)
                WHERE 
                    latitude IS NULL 
                    AND farm_device_id IN (SELECT farm_device_id FROM tb_farm_device)
            """)
            conn.commit()
        except Exception as e:
            print("tb_farmtos_of_collect_fdr Insert Error:", e)


async def db_of_writer():
    sensor_ids = {"rs485_1":24, "rs485_2":18, "a0":23, "a1":25, "a2":19, "a3":20}

    while True:
        await asyncio.sleep(2)
        if db_of_buffer:
            items = db_of_buffer[:]
            db_of_buffer.clear()
            
            try:
                conn = sqlite3.connect(OF_DB_FILE)
                cur = conn.cursor()
                
                fdr_items = []
                solar_items = []
                weather_station_items = []
                other_items = []

                for item in items:
                    sensor_type = item[0]
                    if "rs485_1" in sensor_type:
                        fdr_items.append(item[1])
                    elif "rs485_2" in sensor_type:
                        solar_items.append(item[1])
                    elif "a0" in sensor_type:
                        weather_station_items.append(item[1])
                    elif "a1" in sensor_type: 
                        solar_items.append(item[1])
                    elif "a2" in sensor_type:   
                        weather_station_items.append(item[1])
                    elif "a3" in sensor_type:   
                        weather_station_items.append(item[1])
                    else:
                        other_items.append(item)

                print(f"--- fdr_items ({len(fdr_items)}건) ---")
                insert_fdr_data(cur, conn, fdr_items, sensor_ids)
                
                print(f"--- solar_items ({len(solar_items)}건) ---")
                # 별도 분리된 함수로 solar_items db 등록 호출
                insert_solar_data(cur, conn, solar_items, sensor_ids)

                
                print(f"--- weather_station_items ({len(weather_station_items)}건) ---")
                insert_weather_station_data(cur, conn, weather_station_items, sensor_ids)

                print(f"--- other_items ({len(other_items)}건) ---")
                
                created_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                for i in other_items: print(i)
                
                print(f"{'=' * 20} {created_date_time} {'=' * 20}")

                # if other_items:
                #     print(f"--- other_items ({len(other_items)}건) ---")
                #     for i in other_items: print(i)

                # tb_farmtos_of_collect_fdr | soil_temperature_value | soil_humidity_value | soil_ec
                # tb_farmtos_of_collect_solar | solar | accumulated_solar
                # tb_farmtos_of_collect_weather_station | outside_co2 | outside_wind_direction | outside_wind_speed
                # 

                # print("db_of_buffer: ", len(items), items[0])
                
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
    # asyncio.create_task(db_writer())
    asyncio.create_task(db_of_writer())
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

@app.get("/export", response_class=HTMLResponse)
async def export_page():
    html_content = """
    <!DOCTYPE html>
    <html lang="ko">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Data Export</title>
        <script src="https://cdn.tailwindcss.com"></script>
    </head>
    <body class="bg-gray-100 flex items-center justify-center h-screen">
        <div class="bg-white p-8 rounded-lg shadow-md w-96">
            <h2 class="text-2xl font-bold mb-6 text-center text-gray-800">센서 데이터 엑셀 백업</h2>
            <form action="/export/excel" method="post" class="space-y-4" onsubmit="setTimeout(() => document.getElementById('password').value = '', 100)">
                <div>
                    <label for="password" class="block text-sm font-medium text-gray-700">관리자 비밀번호</label>
                    <input type="password" id="password" name="password" required
                        class="mt-1 block w-full px-3 py-2 bg-white border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500">
                </div>
                <button type="submit"
                    class="w-full flex justify-center py-2 px-4 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500">
                    Export 실행
                </button>
            </form>
        </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)

@app.post("/export/excel")
async def export_data(password: str = Form(...)):
    global is_active
    
    if password != ADMIN_PASSWORD:
        return HTMLResponse(content="<script>alert('비밀번호가 일치하지 않습니다.'); window.history.back();</script>")
    
    # 데이터 수집 일시 중지
    was_active = is_active
    is_active = False
    
    try:
        # export 디렉토리 생성
        os.makedirs("./export", exist_ok=True)
        
        # 엑셀 파일명 생성
        today_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"./export/{today_str}_sensor_data_raw.xlsx"
        
        # DB에서 데이터 읽기 (설정된 기간만큼)
        time_ago = (datetime.now() - timedelta(days=EXPORT_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
        conn = sqlite3.connect(DB_FILE)
        cur = conn.cursor()
        cur.execute(
            "SELECT id, sensor_type, measured_value, raw_value, sensor_line, collector_id, created_at FROM sensor_data WHERE created_at >= ? ORDER BY id ASC",
            (time_ago,)
        )
        rows = cur.fetchall()
        
        # openpyxl로 엑셀 쓰기
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Sensor Data"
        
        # 헤더 쓰기
        ws.append(["ID", "Sensor Type", "Measured Value", "Raw Value", "Sensor Line", "Collector ID", "Created At"])
        
        # 데이터 쓰기
        for row in rows:
            ws.append(row)
            
        wb.save(file_path)
        conn.close()
        
        return FileResponse(
            path=file_path, 
            filename=f"{today_str}_sensor_data_raw.xlsx", 
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
    except Exception as e:
        print("Export Error:", e)
        return HTMLResponse(content=f"<script>alert('엑셀 변환 중 오류가 발생했습니다: {str(e)}'); window.history.back();</script>")
        
    finally:
        # 데이터 수집 상태 복구
        is_active = was_active

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
    uvicorn.run("sensor_consumer:app", host="0.0.0.0", port=CONSUMER_PORT, reload=True)

# Farmtos 센서정보 수집 (Sensor-Publish-Consumer)

## 프로젝트 개요
이 프로젝트는 시리얼 통신을 통해 농업용 센서 데이터를 수집하고, MQTT 프로토콜을 활용하여 데이터를 발행(Publish) 및 구독(Consume)하여 데이터베이스에 저장하는 파이프라인입니다. 

### 주요 구성 요소
1. **Message Broker (`docker-compose.yml`)**:
   - RabbitMQ를 사용하여 MQTT 통신 환경을 제공합니다. 

2. **MQTT Publisher (`mqtt_publisher.py`)**: 
   - 시리얼 포트(RS485/Modbus 등)를 통해 아두이노 등 센서 디바이스로부터 데이터를 비동기적으로 읽어옵니다.
   - 수집된 원시 데이터를 지정된 MQTT 토픽으로 발행합니다.
   - 로컬 로그 파일에 센서 데이터를 백업 기록합니다. (RotatingFileHandler 사용)
   - FastAPI를 기반으로 상태 확인 및 수집 제어(Toggle) API를 제공합니다.

3. **MQTT Consumer (`main.py`)**:
   - MQTT 브로커(RabbitMQ)에 구독하여 센서 데이터를 실시간으로 수신합니다.
   - 수신된 JSON 형태의 데이터를 파싱하고 버퍼링하여 SQLite 데이터베이스(`base_sensor_raw.db`)에 일괄 삽입(Bulk Insert)합니다.
   - FastAPI를 기반으로 센서 수집 통계, 상태 조회 및 처리 제어 API를 제공합니다.
   - 관리자 인증을 통해 수집된 데이터를 Excel(`.xlsx`) 파일로 추출하고 다운로드할 수 있는 웹 인터페이스를 제공합니다.

## 환경 설정 및 실행 방법

### 1. 요구 사항 (Prerequisites)
- Python 3.8 이상
- Docker 및 Docker Compose (RabbitMQ 실행용)

### 2. 패키지 설치
가상 환경을 생성하고 필요한 파이썬 패키지를 설치합니다.
```bash
# 가상환경 생성
python -m venv .venv

# 가상환경 활성화 (Windows)
.venv\Scripts\activate

# 가상환경 활성화 (Mac/Linux)
source .venv/bin/activate

pip install -r requirements.txt
```

### 3. 환경 변수 설정 (`.env`)
프로젝트 루트 디렉토리에 `.env` 파일을 생성하거나 수정하여 환경에 맞게 설정합니다.
```env
SERIAL_PORT=COM6           # 센서와 연결된 시리얼 포트
SERIAL_BAUDRATE=115200     # 통신 속도
MQTT_HOST=localhost        # MQTT 브로커 호스트
MQTT_PORT=1883             # MQTT 포트
MQTT_USER=admin            # MQTT 사용자명
MQTT_PASSWORD=admin        # MQTT 비밀번호
MQTT_TOPIC=farm/sensors/all # 발행/구독할 MQTT 토픽
LOG_MAX_BYTES=2097152      # 최대 로그 파일 크기 (Bytes)
LOG_BACKUP_COUNT=5         # 보관할 로그 파일 개수
PUBLISHER_PORT=8000        # Publisher API 포트
CONSUMER_PORT=8001         # Consumer API 포트
STATS_INTERVAL_MINUTES=10  # 통계 주기 (분)
ADMIN_PASSWORD=6694700477  # Export 웹 페이지 관리자 비밀번호
EXPORT_DAYS=2              # 엑셀 데이터 추출 대상 기간 (최근 N일)
```

### 4. 서비스 실행

#### 4.1. MQTT 브로커 (RabbitMQ) 실행
Docker Compose를 사용하여 백그라운드에서 RabbitMQ를 실행합니다.
```bash
docker-compose up -d
```

#### 4.2. Publisher 및 Consumer 실행
두 개의 파이썬 스크립트를 각각 실행합니다.
```bash
# Publisher 실행
python mqtt_publisher.py  # 포트 8000에서 FastAPI 서버 실행됨

# Consumer 실행
python main.py  # 포트 8001에서 FastAPI 서버 실행됨
```
*(코드 내 `__main__` 블록을 통해 uvicorn 서버가 자동 실행됩니다.)*

## API 엔드포인트
- **Publisher (포트 8000)**
  - `GET /health` : 서비스 상태 및 활성화 여부 확인
  - `GET /api/status` : 수집 활성화 상태 반환
  - `POST /api/toggle` : 데이터 수집 시작/중지 토글

- **Consumer (포트 8001)**
  - `GET /health` : 서비스 상태, DB 저장 건수 및 디스크 용량 확인
  - `GET /api/status` : 수신 활성화 상태 반환
  - `GET /api/stats` : 센서 라인별 데이터 저장 통계 확인
  - `POST /api/toggle` : 데이터 저장 시작/중지 토글
  - `GET /export` : 엑셀 백업 다운로드를 위한 관리자 인증 웹 페이지
  - `POST /export/excel` : 관리자 비밀번호 확인 후 엑셀 파일 생성 및 다운로드


## Github 초기 설정 참고 (기존 내용 유지)
```bash
echo "# sensor-publish-consumer" >> README.md
git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/esummer9/sensor-publish-consumer.git
git push -u origin main
```
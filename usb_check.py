import serial
import time

try:
    # 아두이노 장치 경로와 보드레이트(Baudrate) 설정
    ser = serial.Serial('/dev/ttyUSB1', 9600, timeout=1)
    time.sleep(2) # 연결 후 아두이노 리셋 대기
    
    if ser.is_open:
        print("아두이노 연결 성공!")
        # 아두이노로 데이터 전송 테스트
        ser.write(b'Hello Arduino\n')
        
        # 아두이노로부터 응답 읽기
        line = ser.readline().decode('utf-8').rstrip()
        print(f"응답: {line}")
        
except Exception as e:
    print(f"연결 오류: {e}")

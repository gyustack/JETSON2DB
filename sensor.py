import logging
import time
import os
import json
from datetime import datetime, timezone
from pymodbus.client import ModbusSerialClient  # 시리얼(USB) 통신용

# 1. 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# 젯슨 USB 포트 (환경에 따라 /dev/ttyUSB0, /dev/ttyACM0 등)
SERIAL_PORT = '/dev/ttyUSB0'

# FIFO 경로 (C가 읽는 통로)
FIFO_PATH = "/tmp/th_fifo"

def ensure_fifo(path: str):
    """FIFO가 없으면 생성 (한 번만)."""
    if not os.path.exists(path):
        os.mkfifo(path)
        os.chmod(path, 0o666)

def open_fifo_writer_blocking(path: str):
    """
    FIFO writer를 엶.
    - C reader가 먼저 열려 있어야 여기서 안 막힘
    - 막히면 정상 동작(리더를 기다리는 중)
    """
    logger.info(f"📌 FIFO 열기 대기 중: {path} (C reader를 먼저 실행하세요)")
    return open(path, "w", buffering=1)  # line-buffered

def run_sensor_node():
    ensure_fifo(FIFO_PATH)

    # 2. 시리얼 클라이언트 생성 (Modbus RTU)
    client = ModbusSerialClient(
        port=SERIAL_PORT,
        baudrate=9600,
        timeout=3
    )

    logger.info(f"🚀 젯슨 USB 데이터 수집 시작 (포트: {SERIAL_PORT})")

    fifo = None

    try:
        # ✅ FIFO writer 오픈 (C reader가 먼저 떠 있어야 막히지 않음)
        fifo = open_fifo_writer_blocking(FIFO_PATH)
        logger.info("✅ FIFO 연결 완료 (Python → C 버퍼링 시작)")

        while True:
            if client.connect():
                # ID 1번 센서의 0번 주소부터 2개 읽기 (0: 온도, 1: 습도)
                result = client.read_input_registers(address=0, count=2, device_id=1)

                if not result.isError():
                    # 시간 문자열(화면 출력용)
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    # 데이터 스케일링 (/10.0)
                    temp = result.registers[0] / 10.0
                    humi = result.registers[1] / 10.0

                    # 터미널 출력(기존 유지)
                    logger.info(f"[{timestamp}] 🌡️ 온도: {temp}°C | 💧 습도: {humi}%")

                    # ✅ FIFO로 JSON 한 줄 전송 (C가 읽어서 CSV로 저장)
                    payload = {
                        "deviceId": "jetson01",
                        "ts": datetime.now(timezone.utc).isoformat(),  # UTC 기준
                        "temperatureC": round(temp, 2),
                        "humidityPct": round(humi, 2)
                    }
                    fifo.write(json.dumps(payload) + "\n")

                else:
                    logger.error(f"[{datetime.now().strftime('%H:%M:%S')}] 센서 응답 에러")

            else:
                logger.error("USB 컨버터 연결 실패. 포트와 권한을 확인하세요.")

            time.sleep(2)

    except BrokenPipeError:
        logger.error("❌ FIFO reader(C)가 종료되어 파이프가 끊겼습니다. C 프로그램을 다시 실행하세요.")
    except KeyboardInterrupt:
        logger.info("수집을 종료합니다.")
    finally:
        try:
            if fifo:
                fifo.close()
        except Exception:
            pass
        client.close()

if __name__ == "__main__":
    run_sensor_node()

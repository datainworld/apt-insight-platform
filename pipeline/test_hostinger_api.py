import os
import requests
from dotenv import load_dotenv

def test_hostinger_api():
    # .env 파일 로드
    load_dotenv()
    
    # '.env'에서 토큰 가져오기 (대소문자 일치)
    api_token = os.environ.get("hostinger_API_token")
    
    if not api_token:
        print("❌ .env 파일에서 hostinger_API_token을 찾을 수 없습니다.")
        return

    # Hostinger API 기본 정보
    base_url = "https://developers.hostinger.com/api/v1"  # 또는 https://api.hostinger.com/v1
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    print("🔌 Hostinger API 통신을 시작합니다...\n")

    # 1. 서버(VPS) 목록 불러오기 테스트
    endpoints_to_test = [
        "https://api.hostinger.com/vps/v1/virtual-machines",
        "https://api.hostinger.com/api/vps/v1/virtual-machines",
        "https://api.hostinger.com/v1/vps/virtual-machines"
    ]
    
    for url in endpoints_to_test:
        print(f"📡 요청 보내는 곳: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=10)
            print(f"응답 코드: {response.status_code}")
            
            if response.status_code == 200:
                print("✅ 연결 성공! 아래는 결과 데이터입니다:")
                data = response.json()
                print(data)
                break # 성공 시 루프 탈출
            elif response.status_code == 401:
                print("❌ 연결 실패 (401): API 토큰이 유효하지 않거나 권한이 없습니다.")
            elif response.status_code == 404:
                print("❌ 연결 실패 (404): 해당 주소가 올바르지 안습니다.")
            else:
                print(f"⚠️ 기타 상태: {response.text[:200]}")
        except Exception as e:
            print(f"오류 발생: {e}")
        print("-" * 40)

if __name__ == "__main__":
    test_hostinger_api()

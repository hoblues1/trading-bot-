from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import time

chrome_options = Options()

# 1. 자동화 제어 정보 숨기기 (핵심)
chrome_options.add_argument('--disable-blink-features=AutomationControlled')
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

# 2. 일반 사용자 환경처럼 보이게 설정
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

service = Service() # 크롬드라이버 경로가 설정되어 있어야 합니다.
driver = webdriver.Chrome(service=service, options=chrome_options)

# 3. 브라우저 내 'webdriver' 변수 제거 (서버 탐지 방지)
driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
    "source": """
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        })
    """
})

# 한화이글스 공식 홈페이지 또는 티켓링크 접속
driver.get("https://www.hanwhaeagles.co.kr/index.do")

# 이후 예매 로직 작성...

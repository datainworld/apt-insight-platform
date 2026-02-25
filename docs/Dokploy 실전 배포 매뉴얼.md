# 🚀 아파트 데이터 수집 시스템 Dokploy 실전 배포 매뉴얼

본 매뉴얼은 GitHub에 푸시된 프로젝트를 Hostinger VPS 상의 Dokploy에 배포하고, 초기 데이터를 수집(컨테이너 백그라운드 구동)한 뒤, 매일 자동 실행되도록 설정하는 전 과정을 담고 있습니다.

## 1. 사전 준비 (Local & GitHub)
1. 로컬 환경에서 코드를 작성하고 테스트를 완료합니다.
2. 제외해야 할 파일(특히 `datas/` 폴더 내의 대용량 CSV)이 `.gitignore`에 잘 등록되어 있는지 확인합니다.
3. 프로젝트 루트에 의존성 패키지 목록을 담은 파일(`pyproject.toml` 또는 `requirements.txt`)이 있어야 합니다.
4. 모든 코드를 GitHub 저장소(Repository)에 Push 합니다.

## 2. Dokploy 애플리케이션 생성 및 빌드
1. **Hostinger VPS**에 설치된 **Dokploy 대시보드**에 접속합니다.
2. **Applications** 탭에서 **Create** 버튼을 눌러 새 애플리케이션을 생성합니다.
3. **Repository 탭 설정:**
   - Provider: Github
   - Repository: 푸시했던 프로젝트 선택 (예: `datainworld/apt-data-collection-manipulation`)
   - Branch: `main` (또는 배포할 브랜치)
4. **Environment 탭 설정 (.env 처리):**
   - 아래의 환경 변수를 모두 복사하여 붙여넣고 저장합니다.
     ```env
     DATA_API_KEY=발급받은_공공데이터_키
     KAKAO_API_KEY=발급받은_카카오_키
     POSTGRES_USER=postgres
     POSTGRES_PASSWORD=비밀번호
     POSTGRES_HOST=host.docker.internal (혹은 로컬 DB 컨테이너 IP)
     POSTGRES_PORT=5432
     POSTGRES_DB=apt_data
     ```
5. **Deployments 탭 (중요 - 컨테이너 유지 설정):**
   - 이 앱은 계속 띄워두는 웹 서버가 아니기 때문에, 빌드가 끝나도 컨테이너가 죽지 않도록 **Start Command**를 입력해야 합니다.
   - **Start Command:** `sleep infinity`
6. **Deploy 버튼**을 눌러 빌드를 시작합니다. (Dokploy가 Nixpacks 빌더를 통해 자동으로 파이썬 환경을 구성합니다.)

---

## 3. Dokploy 컨테이너 내부 파일 구조 이해
배포가 완료된 후 컨테이너 내부의 구조는 다음과 같이 형성됩니다. (Nixpacks 빌드 기준)

```text
/
├── app/                      # 프로젝트 코드가 위치하는 실제 작업 디렉토리 (기본 실행 경로)
│    ├── collect_and_process.py
│    ├── update_and_migrate.py
│    ├── pyproject.toml
│    ├── utils.py
│    └── datas/               # 수집된 데이터가 저장될 폴더 (실행 시 생성됨)
│
├── opt/
│    └── venv/                # (참고) Dokploy가 기본으로 생성하는 파이썬 가상 환경
│
├── usr/
│    └── bin/
│         └── python3         # ⭐️ 우리가 최종적으로 사용할 Ubuntu 순정 파이썬
│
└── root/
     └── .nix-profile/bin/    # (참고) Nixpacks가 설치한 격리된 동작 환경 (의존성 충돌의 원인)
```

---

## 4. 서버 SSH 접속 및 1회성 데이터 최초 수집 실행
위 구조에서 보았듯, Dokploy가 임의로 만든 파이썬 환경(`/root/.../python3` 또는 가상환경)을 쓰면 C++ 빌드 라이브러리(`libstdc++6`) 누락 등으로 Numpy, Pandas가 깨지는 치명적 문제가 발생할 수 있습니다. 

따라서 **가장 안전한 우분투(Ubuntu) 순정 파이썬 패키지를 강제 설치하여 수집을 진행**합니다.

1. 개발자 PC의 터미널(CMD, PowerShell 등)에서 Hostinger 서버로 SSH 접속합니다.
   ```bash
   ssh root@서버아이피
   ```
2. 현재 실행 중인 Dokploy 컨테이너의 ID를 확인합니다.
   ```bash
   docker ps
   # 'apt-data-collect...' 이름의 컨테이너 ID 복사 (예: 46dc7e4195e3)
   ```
3. 컨테이너 내부 쉘로 들어갑니다. (명령어에 복사한 ID 입력)
   ```bash
   docker exec -it <컨테이너ID> /bin/bash
   docker exec -it 46dc7e4195e3 /bin/bash
   docker exec -it 1ccf0749f56c /bin/bash 
   # 접속 후 프롬프트가 root@<컨테이너ID>:/app# 로 변경됨
   ```

4. **(핵심 트러블슈팅)** 우분투 OS의 시스템 파이썬(`python3`)에 패키지를 강제로 모두 설치 시킵니다. (`--break-system-packages` 옵션 사용)
   ```bash
   apt-get update
   apt-get install -y python3-pip libstdc++6 gcc make g++
   python3 -m pip install pandas requests xmltodict python-dotenv sqlalchemy psycopg2-binary python-dateutil curl-cffi --break-system-packages
   ```
5. 완벽하게 준비된 `/usr/bin/python3` 절대 경로를 명시하여 백그라운드 수집을 시작합니다. (약 8~12시간 소요)
   ```bash
   nohup /usr/bin/python3 collect_and_process.py --regions 11 28 41 --months 36 > collect.log 2>&1 &
   ```
6. 데이터가 무사히 수집되고 있는지 실시간 로그를 확인합니다.
   ```bash
   tail -f collect.log
   ```
7. 문제없이 수집 로그가 찍힌다면 `Ctrl + C`를 눌러 로그 화면을 빠져나온 후, `exit`를 쳐서 컨테이너에서 나오고 다시 서버 연결을 종료해도 됩니다.

> [!TIP] **API 일일 제한(Rate Limit) 대응**
> K-Apt 기본/상세 정보 수집은 하루 10,000건 호출 제한이 있습니다. 스크립트에는 이미 `--max-basic 10000`이 기본 적용되어 있어 1만 건 도달 시 다음 단계로 자동 넒어갑니다.
> 만약 특정 단계만 단독으로 다시 돌려야 할 경우 `--skip-code`, `--skip-basic`, `--skip-trade` 옵션을 조합하여 불필요한 단계를 스킵할 수 있습니다.
> 예시: `nohup ... collect_and_process.py --skip-code --skip-basic`

---

## 5. (중요) 데이터 보존을 위한 Volume 설정
컨테이너 기반 환경인 Dokploy는 코드가 업데이트되어 새로 배포될 때마다 내부 파일(`datas/` 등)이 리셋됩니다. 이를 막기 위해 반드시 저장소 볼륨 마운트가 필요합니다.

1. Dokploy 대시보드의 Application 화면에서 **Advanced** (또는 Volumes/Storage) 탭으로 이동
2. 아래 정보로 볼륨을 **Add** 합니다:
   - **Type**: Bind (또는 Mount)
   - **Host Path**: `/var/lib/dokploy/apt-datas` (Hostinger 서버 물리 경로, 임의 지정 가능)
   - **Mount Path**: `/app/datas` (컨테이너 내 데이터 저장 경로)
3. **Save** 후 애플리케이션을 다시 **Deploy** 합니다. 이제 컨테이너가 100번 재시작해도 수집된 마스터 CSV 데이터는 무조건 안전하게 유지됩니다!

---

## 6. 일일 작동 설정 (Dokploy 스케줄러 등록)
초기 거대한 데이터 수집이 끝난 뒤, 매일매일 자동으로 어제까지의 데이터를 증분 업데이트하고 DB에 밀어 넣으려면 **Dokploy 대시보드의 Cron Jobs**를 활용합니다. (서버에서 crontab을 칠 필요가 없습니다!)

1. Dokploy 대시보드 - 좌측 메뉴 중 **Scheduled Jobs**로 이동합니다. (버전에 따라 해당 Application 요약화면의 Jobs 탭에 있을 수 있습니다.)
2. **실거래가 매일 업데이트 Job 생성**
   - **Name:** Daily_Apt_Update
   - **Cron Expression:** `0 2 * * *` (매일 새벽 2시)
   - **Command:** `/usr/bin/python3 /app/update_and_migrate.py`
3. **네이버 부동산 매일 수집 Job 생성**
   - **Name:** Daily_Naver_Update
   - **Cron Expression:** `0 4 * * *` (매일 새벽 4시)
   - **Command:** `/usr/bin/python3 /app/collect_naver_listing.py`

✅ **완료!** 
이제 GitHub에 코드를 푸시하면 새로운 수집 스크립트가 배포되며, 볼륨 마운트를 통해 데이터 유실 없이 매일 설정된 시간에 안전하게 자동 수집 시스템이 돌아가는 최적의 아키텍처가 완성되었습니다.

# pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib gspread pandas
from gspread.utils import rowcol_to_a1

import time
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
gc = gspread.authorize(creds)

RESPONSE_SHEET_ID = "1v-QMP93fTGfWuoIM1pGZMKbfcXvUrtjl9reA8JDSJqk"
ADMIN_SHEET_ID = "1Kq_A2WLDtfboUP6pu9xI3TsNrOpxfCvLcoyFIi8QOxg"

response_sheet_name = "설문지 응답 시트1"
copied_sheet_name = "Form_Responses(백업)"
admin_sheet_name = (
    "의왕청계2 A1BL 입주(계약)자 확인 및 관리시트(관리자)"
)

response_ws = gc.open_by_key(RESPONSE_SHEET_ID).worksheet(response_sheet_name)
admin_ws = gc.open_by_key(ADMIN_SHEET_ID).worksheet(admin_sheet_name)


def get_worksheet_by_id_with_retry(spreadsheet, sheet_id, retries=5, delay=1):
    for i in range(retries):
        try:
            sheet = spreadsheet.get_worksheet_by_id(sheet_id)
            if sheet:
                return sheet
        except Exception:
            pass
        time.sleep(delay)
    raise RuntimeError(f"복사된 시트를 찾을 수 없습니다: sheetId={sheet_id}")


def safe_delete_sheet_by_title(spreadsheet, title):
    """동일한 이름의 시트가 있으면 삭제"""
    try:
        sheet = spreadsheet.worksheet(title)
        spreadsheet.del_worksheet(sheet)
        print(f"🗑 기존 시트 '{title}' 삭제 완료")
    except gspread.exceptions.WorksheetNotFound:
        pass  # 삭제할 시트가 없다면 무시


def copy_response_sheet_to_admin_sheet():
    # 복사 수행
    source = gc.open_by_key(RESPONSE_SHEET_ID)
    response_sheet = source.worksheet(response_sheet_name)
    copied = response_sheet.copy_to(ADMIN_SHEET_ID)

    # 복사된 시트 객체 재시도 방식으로 얻기
    dest_book = gc.open_by_key(ADMIN_SHEET_ID)

    # 동일 이름 시트 삭제
    safe_delete_sheet_by_title(dest_book, copied_sheet_name)
    copied_ws = get_worksheet_by_id_with_retry(dest_book, copied["sheetId"])

    # 이름 변경
    copied_ws.update_title(copied_sheet_name)
    print(f"✅ 시트 복사 및 이름 변경 완료: {copied_sheet_name}")


def merge_into_admin_sheet():
    # 관리자 시트 데이터 불러오기
    expected_headers = [
        "동",
        "호수",
        "타입",
        "이름",
        "비상연락망",
        "카카오톡 안내(세대별 1인, 2인부터는 일정기간 후 참여가능)",
        "카카오톡 닉네임",
        "네이버카페 ID",
        "계약서 업로드",
        "입주예정자협의회 위임장 업로드",
        "비고",
    ]
    admin_df = pd.DataFrame(
        admin_ws.get_all_records(expected_headers=expected_headers, head=3)
    )
    admin_df = admin_df.astype(str)

    # 응답 시트 데이터 불러오기
    response_df = pd.DataFrame(response_ws.get_all_records())

    if response_df.empty:
        print("⚠️ 응답 시트에 데이터가 없습니다. 병합을 건너뜁니다.")
        return

    # Key 생성
    admin_df["KEY"] = (
        admin_df["동"].astype(str).str.strip()
        + "-"
        + admin_df["호수"].astype(str).str.strip()
    )
    response_df["KEY"] = (
        response_df["동"].astype(str).str.strip()
        + "-"
        + response_df["호수"].astype(str).str.strip()
    )
    response_df.set_index("KEY", inplace=True)

    # 병합
    for i, row in admin_df.iterrows():
        key = row["KEY"]
        if key not in response_df.index:
            continue

        res_list = response_df.loc[[key]]
        for _, res in res_list.iterrows():
            def merge_column(res_col, admin_col):
                if res_col in res and admin_col in admin_df.columns:
                    original = str(admin_df.at[i, admin_col]).strip()
                    new = str(res[res_col]).strip()
                    items = set(filter(None, [original, new]))
                    admin_df.at[i, admin_col] = ", ".join(sorted(items))

            merge_column("이름", "이름")
            merge_column("비상연락망", "비상연락망")
            merge_column("네이버카페 ID", "네이버카페 ID")
            merge_column("계약서 업로드", "계약서 업로드")
            merge_column("위임장 업로드", "입주예정자협의회 위임장 업로드")

    protected_cols = [
        "카카오톡 안내(세대별 1인, 2인부터는 일정기간 후 참여가능)",
        "카카오톡 닉네임",
    ]

    # ✨ 업데이트할 값 생성
    update_values = []
    for _, row in admin_df.iterrows():
        row_values = []
        is_empty = True

        for col in expected_headers:
            if col in protected_cols:
                row_values.append("")  # 보호 컬럼은 공란으로 유지
            else:
                val = row.get(col, "")
                val_str = "" if pd.isna(val) else str(val).strip()
                row_values.append(val_str)
                if val_str:
                    is_empty = False

        if not is_empty:
            update_values.append(row_values)

    # ⛔ 비어 있으면 건너뜀
    if not update_values:
        print("업데이트할 내용이 없습니다.")
    else:
        # 🧮 B4 기준 범위 계산
        start_col_letter = "B"
        start_cell = "B4"
        end_col_letter = chr(ord(start_col_letter) + len(expected_headers) - 1)
        end_row = 3 + len(update_values) + 1
        range_name = f"{start_cell}:{end_col_letter}{3 + len(update_values)}"

        print(f"🟢 Updating range: {range_name}")
        admin_ws.update(range_name=range_name, values=update_values)

def main():
    copy_response_sheet_to_admin_sheet()
    merge_into_admin_sheet()


if __name__ == "__main__":
    main()

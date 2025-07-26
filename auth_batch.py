#pip install --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib gspread pandas

import os
import json
import time
import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd


@dataclass
class SheetConfig:
    """Configuration for Google Sheets integration"""
    response_sheet_id: str = "1v-QMP93fTGfWuoIM1pGZMKbfcXvUrtjl9reA8JDSJqk"
    admin_sheet_id: str = "1Kq_A2WLDtfboUP6pu9xI3TsNrOpxfCvLcoyFIi8QOxg"
    response_sheet_name: str = "설문지 응답 시트1"
    copied_sheet_name: str = "Form_Responses(백업)"
    admin_sheet_name: str = "의왕청계2 A1BL 입주(계약)자 확인 및 관리시트(관리자)"
    scopes: List[str] = None
    
    def __post_init__(self):
        if self.scopes is None:
            self.scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]


class GoogleSheetsManager:
    """Google Sheets operations manager"""
    
    def __init__(self, config: SheetConfig):
        self.config = config
        self.client = self._setup_client()
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
        
    def _setup_client(self) -> gspread.Client:
        """Setup Google Sheets client with credentials"""
        try:
            creds = Credentials.from_service_account_file("credentials.json", scopes=self.config.scopes)
            return gspread.authorize(creds)
        except Exception as e:
            raise RuntimeError(f"Failed to setup Google Sheets client: {e}")

    def get_worksheet_by_id_with_retry(self, spreadsheet: gspread.Spreadsheet, sheet_id: str, retries: int = 5, delay: int = 1) -> gspread.Worksheet:
        """Get worksheet by ID with retry mechanism"""
        for attempt in range(retries):
            try:
                sheet = spreadsheet.get_worksheet_by_id(sheet_id)
                if sheet:
                    return sheet
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1}/{retries} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
        raise RuntimeError(f"복사된 시트를 찾을 수 없습니다: sheetId={sheet_id}")

    def safe_delete_sheet_by_title(self, spreadsheet: gspread.Spreadsheet, title: str) -> None:
        """Delete sheet by title if it exists"""
        try:
            sheet = spreadsheet.worksheet(title)
            spreadsheet.del_worksheet(sheet)
            self.logger.info(f"🗑 기존 시트 '{title}' 삭제 완료")
        except gspread.exceptions.WorksheetNotFound:
            self.logger.info(f"시트 '{title}'가 존재하지 않아 삭제를 건너뜁니다.")


    def _get_protected_columns(self) -> List[str]:
        """Get protected columns that should not be updated"""
        return [
            "검토자1", "검토자2", "동", "호수", "타입", "비고","카카오톡 닉네임+uuid"
        ]
    
    def _read_sheets(self) -> tuple[List[Dict], List[Dict], gspread.Worksheet]:
        """Read both response and admin sheets and return their data with admin worksheet"""
        try:
            self.logger.info("응답 시트와 관리자 시트 열기 시작...")
            
            # Open response sheet
            response_sheet = self.client.open_by_key(self.config.response_sheet_id)
            response_ws = response_sheet.worksheet(self.config.response_sheet_name)
            response_data = response_ws.get_all_records()
            self.logger.info(f"✅ 응답 시트 읽기 완료 - {len(response_data)}개 레코드")
            
            # Open admin sheet
            admin_sheet = self.client.open_by_key(self.config.admin_sheet_id)
            admin_ws = admin_sheet.worksheet(self.config.admin_sheet_name)
            admin_data = admin_ws.get_all_records()
            self.logger.info(f"✅ 관리자 시트 읽기 완료 - {len(admin_data)}개 레코드")
            
            # Log sample data for verification
            if response_data:
                self.logger.info(f"응답 시트 컬럼: {list(response_data[0].keys())}")
            if admin_data:
                self.logger.info(f"관리자 시트 컬럼: {list(admin_data[0].keys())}")
            
            return response_data, admin_data, admin_ws
                
        except Exception as e:
            self.logger.error(f"❌ 시트 읽기 실패: {e}")
            raise
    
    def backup_admin_sheet(self, admin_ws: gspread.Worksheet) -> None:
        """Backup admin sheet before processing"""
        try:
            from datetime import datetime
            
            # Create backup sheet name with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"Admin_Backup_{timestamp}"
            
            self.logger.info(f"관리자 시트 백업 시작: {backup_name}")
            
            # Copy to backup
            copied = admin_ws.copy_to(self.config.admin_sheet_id)
            
            # Get the copied sheet and rename it
            admin_sheet = self.client.open_by_key(self.config.admin_sheet_id)
            copied_ws = self.get_worksheet_by_id_with_retry(admin_sheet, copied["sheetId"])
            copied_ws.update_title(backup_name)
            
            self.logger.info(f"✅ 관리자 시트 백업 완료: {backup_name}")
            
        except Exception as e:
            self.logger.error(f"❌ 관리자 시트 백업 실패: {e}")
            raise
    
    def _create_key(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create composite key from 동 and 호수 columns"""
        df["KEY"] = (
            df["동"].astype(str).str.strip() + "-" + 
            df["호수"].astype(str).str.strip()
        )
        return df
    
    def _get_column_mappings(self) -> Dict[str, str]:
        """Get direct column mappings between response and admin sheets"""
        return {
            "위임장 업로드": "위임장 업로드",
            "계약서 업로드": "계약서 업로드", 
            "이름": "이름",
            "비상연락망": "비상연락망",
            "네이버카페 ID": "네이버카페 ID",
            "자격구분": "자격구분",
            "세대 대표자 여부": "세대 대표자 여부",
            "개인정보 수집·이용 동의": "개인정보 수집·이용 동의"
        }
    
    def _merge_multiline_data(self, existing_value: str, new_values: List[str]) -> str:
        """Merge existing value with new values in multiline format"""
        all_values = []
        
        # Add existing values if any
        if existing_value and str(existing_value).strip():
            existing_lines = [line.strip() for line in str(existing_value).split('\n') if line.strip()]
            all_values.extend(existing_lines)
        
        # Add new values
        for value in new_values:
            value_str = str(value).strip()
            if value_str:
                all_values.append(value_str)
        
        # Remove duplicates while preserving order
        unique_values = []
        seen = set()
        for value in all_values:
            if value not in seen:
                unique_values.append(value)
                seen.add(value)
        
        return '\n'.join(unique_values)
    
    def _extract_representatives(self, responses_for_key: List[Dict]) -> str:
        """Extract representative members based on '세대 대표자 여부' column"""
        representatives = []
        for response in responses_for_key:
            is_representative = str(response.get('세대 대표자 여부', '')).strip()
            # Check various possible values for representative status
            if is_representative.lower() in ['예', 'yes', 'y', 'o', '대표', 'true', '1']:
                name = str(response.get('이름', '')).strip()
                if name:
                    representatives.append(name)
        return '\n'.join(representatives) if representatives else ""
    
    def _generate_uuid_from_df(self, responses_df_for_key: pd.DataFrame) -> str:
        """Generate UUID sequence based on actual response sheet row numbers"""
        if responses_df_for_key.empty:
            return ""
        
        # Get the actual row numbers from the DataFrame index
        # Response sheet row number = DataFrame index + 2 (assuming header in row 1, 0-based index)
        row_numbers = []
        for idx in responses_df_for_key.index:
            actual_row = idx + 2  # Convert 0-based index to 1-based row number, +1 for header
            row_numbers.append(str(actual_row))
        
        self.logger.debug(f"UUID 생성: DataFrame indices {list(responses_df_for_key.index)} -> 행번호 {row_numbers}")
        return '\n'.join(row_numbers)

    def merge_into_admin_sheet(self, response_data, admin_data, admin_ws) -> None:
        """Merge response data into admin sheet"""
        try:
            self.logger.info("응답 데이터를 관리자 시트에 병합 시작...")
            
            # Convert to DataFrames
            response_df = pd.DataFrame(response_data)
            admin_df = pd.DataFrame(admin_data)


            if response_df.empty:
                self.logger.warning("⚠️ 응답 시트에 데이터가 없습니다. 병합을 건너뜁니다.")
                return
            
            self.logger.info(f"Response 컬럼: {list(response_df.columns)}")
            self.logger.info(f"Admin 컬럼: {list(admin_df.columns)}")
            
            # Create keys for both dataframes
            response_df = self._create_key(response_df)
            admin_df = self._create_key(admin_df)
            
            # Group response data by KEY (동-호수)
            response_grouped = response_df.groupby('KEY')
            self.logger.info(f"Response 데이터 그룹: {len(response_grouped)} 개 동호수")
            
            column_mappings = self._get_column_mappings()
            protected_cols = self._get_protected_columns()

            # Process each admin row
            updated_count = 0
            for i, admin_row in admin_df.iterrows():
                key = admin_row['KEY']
                
                if key not in response_grouped.groups:
                    continue
                
                # Get all responses for this 동호수
                responses_df_for_key = response_grouped.get_group(key)
                responses_for_key = responses_df_for_key.to_dict('records')
                self.logger.info(f"동호수 {key}: {len(responses_for_key)}개 응답 처리")
                
                # Generate UUID sequence using actual row numbers
                if 'uuid' in admin_df.columns:
                    admin_df.at[i, 'uuid'] = self._generate_uuid_from_df(responses_df_for_key)
                
                # Extract representatives for  ID
                if '대표자 이름' in admin_df.columns:
                    admin_df.at[i, '대표자 이름'] = self._extract_representatives(responses_for_key)
                
                # Process each column mapping
                for response_col, admin_col in column_mappings.items():
                    if admin_col in protected_cols:
                        self.logger.debug(f"보호된 컬럼 건너뜀: {admin_col}")
                        continue
                    
                    if response_col in response_df.columns and admin_col in admin_df.columns:
                        # Collect all values for this column from responses
                        new_values = [str(resp.get(response_col, '')).strip() for resp in responses_for_key]
                        new_values = [v for v in new_values if v]  # Remove empty values
                        
                        if new_values:
                            existing_value = str(admin_df.at[i, admin_col]) if not pd.isna(admin_df.at[i, admin_col]) else ""
                            merged_value = self._merge_multiline_data(existing_value, new_values)
                            admin_df.at[i, admin_col] = merged_value
                
                updated_count += 1
            
            self.logger.info(f"{updated_count}개 동호수 데이터 병합 완료")

            # Update the sheet
            self._update_admin_sheet(admin_df, admin_ws)
            
        except Exception as e:
            self.logger.error(f"데이터 병합 중 오류 발생: {e}")
            raise
    

    def _apply_one_time_formatting(self, admin_ws: gspread.Worksheet, start_row: int, num_rows: int, num_cols: int) -> None:
        """One-time formatting: Apply gray background to odd rows with rate limiting"""
        try:
            self.logger.info("🎨 일회성 포맷팅: 홀수 행에 회색 배경 적용 중 (170행부터 재시작)...")
            
            # Light gray color for alternating rows
            gray_format = {
                "backgroundColor": {
                    "red": 0.95,
                    "green": 0.95, 
                    "blue": 0.95
                }
            }
            
            # Start from row 170 to avoid rate limit issues
            start_from_idx = max(0, 330 - start_row)
            formatted_rows = 0
            
            for row_idx in range(start_from_idx, num_rows):
                actual_row = start_row + row_idx
                
                # Apply to odd rows (1, 3, 5, 7...)
                if actual_row % 2 == 1:
                    start_col_letter = "A"
                    end_col_letter = chr(ord(start_col_letter) + num_cols - 1)
                    range_name = f"{start_col_letter}{actual_row}:{end_col_letter}{actual_row}"
                    
                    try:
                        admin_ws.format(range_name, gray_format)
                        formatted_rows += 1
                        self.logger.info(f"회색 배경 적용: {range_name}")
                        
                        # Rate limiting: wait between requests
                        time.sleep(0.1)  # 100ms delay between each format request
                        
                    except Exception as e:
                        if "quota" in str(e).lower() or "limit" in str(e).lower():
                            self.logger.warning(f"API 제한 도달, 잠시 대기 후 재시도...")
                            time.sleep(2)  # Wait 2 seconds on quota error
                            try:
                                admin_ws.format(range_name, gray_format)
                                formatted_rows += 1
                                self.logger.info(f"재시도 성공: {range_name}")
                            except Exception as retry_e:
                                self.logger.error(f"재시도 실패 {range_name}: {retry_e}")
                                break  # Stop formatting on persistent error
                        else:
                            self.logger.warning(f"포맷팅 실패 {range_name}: {e}")
            
            self.logger.info(f"✅ 일회성 포맷팅 완료: {formatted_rows}개 홀수 행에 회색 배경 적용 (170행부터)")
            
        except Exception as e:
            self.logger.error(f"일회성 포맷팅 중 오류: {e}")
            # 포맷팅 실패해도 데이터는 정상 업데이트된 상태

    def _update_admin_sheet(self, admin_df: pd.DataFrame, admin_ws: gspread.Worksheet) -> None:
        """Update admin sheet with merged data - use existing DataFrame structure"""
        try:
            self.logger.info("관리자 시트 업데이트 시작...")
            
            # Remove KEY column for update
            update_df = admin_df.drop(columns=['KEY'], errors='ignore')
            
            # Debug: Log columns and sample data
            self.logger.info(f"업데이트할 DataFrame 컬럼: {list(update_df.columns)}")
            if not update_df.empty:
                self.logger.info(f"샘플 데이터 (첫번째 행):")
                for col in update_df.columns:
                    sample_value = update_df.iloc[0][col]
                    if str(sample_value).strip():  # Only log non-empty values
                        self.logger.info(f"  {col}: '{sample_value}'")
            
            # Prepare update values
            update_values = []
            for _, row in update_df.iterrows():
                row_values = []
                for col in update_df.columns:
                    val = row[col]
                    val_str = "" if pd.isna(val) else str(val)
                    row_values.append(val_str)
                update_values.append(row_values)
            
            if not update_values:
                self.logger.warning("업데이트할 데이터가 없습니다.")
                return
            
            # Calculate update range - find where data actually starts
            # First, check the sheet structure
            all_data = admin_ws.get_all_values()
            self.logger.info(f"시트 전체 구조 (처음 5행):")
            for i, row in enumerate(all_data[:5], 1):
                self.logger.info(f"  행 {i}: {row}")
            
            # Find the header row (contains column names)
            header_row_idx = None
            for i, row in enumerate(all_data):
                if any('동' in str(cell) and '호수' in str(all_data[i]) for cell in row):
                    header_row_idx = i + 1  # Convert to 1-based
                    break
            
            if header_row_idx is None:
                # Fallback to default
                header_row_idx = 3
                self.logger.warning("헤더 행을 찾을 수 없어 기본값(3행) 사용")
            
            # Data starts right after header
            start_row = header_row_idx + 1
            start_col = "A"
            end_col = chr(ord(start_col) + len(update_df.columns) - 1)
            end_row = start_row + len(update_values) - 1
            range_name = f"{start_col}{start_row}:{end_col}{end_row}"
            
            self.logger.info(f"🟢 헤더 행: {header_row_idx}, 데이터 시작: {start_row}")
            self.logger.info(f"🟢 업데이트 범위: {range_name} ({len(update_values)}행 x {len(update_df.columns)}열)")
            
            # Update the sheet
            admin_ws.update(range_name=range_name, values=update_values)
            
            # One-time formatting: Apply alternating gray colors to odd rows
            # self._apply_one_time_formatting(admin_ws, start_row, len(update_values), len(update_df.columns))
            
            self.logger.info("✅ 관리자 시트 업데이트 완료")
            
        except Exception as e:
            self.logger.error(f"시트 업데이트 중 오류 발생: {e}")
            raise
    
    def process_sheets(self) -> None:
        """Main method to process both copy and merge operations"""
        self.logger.info("Google Sheets 처리 시작")

        # File load confirmation
        try:
            self.logger.info("Credentials 파일 확인 중...")
            # Test connection by opening a sheet
            test_sheet = self.client.open_by_key(self.config.response_sheet_id)
            self.logger.info(f"✅ Credentials 로드 성공 - 시트 '{test_sheet.title}' 접근 확인")
        except Exception as e:
            self.logger.error(f"❌ Credentials 로드 실패: {e}")
            raise
        
        # Test reading both sheets
        response_data, admin_data, admin_ws = self._read_sheets()
        self.logger.info(f"시트 읽기 테스트 완료 - 응답: {len(response_data)}개, 관리자: {len(admin_data)}개 레코드")
        
        # Backup admin sheet before processing
        self.backup_admin_sheet(admin_ws)
        
        self.merge_into_admin_sheet(response_data,admin_data,admin_ws)
        self.logger.info("Google Sheets 처리 완료")


def main():
    """Main function to run the Google Sheets processing"""
    try:
        config = SheetConfig()
        manager = GoogleSheetsManager(config)
        manager.process_sheets()
    except Exception as e:
        logging.error(f"프로그램 실행 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    main()

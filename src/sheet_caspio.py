import gspread
import requests
import time
import os
from oauth2client.service_account import ServiceAccountCredentials

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

class GoogleSheetsCaspioTransfer:
    def __init__(self, caspio_config, google_credentials_path):
        self.caspio_config = caspio_config
        self.google_credentials_path = google_credentials_path
        self.caspio_token = None
        self.gc = None

    def authenticate_google_sheets(self):
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = ServiceAccountCredentials.from_json_keyfile_name(
                self.google_credentials_path, scope)
            self.gc = gspread.authorize(creds)
            print("✓ Google Sheets authentication successful")
            return True
        except Exception as e:
            print(f"✗ Google Sheets authentication failed: {e}")
            return False

    def get_caspio_token(self):
        try:
            account_id = self.caspio_config['account_id'].replace('https://', '').replace('http://', '')
            if account_id.endswith('.caspio.com'):
                account_id = account_id.replace('.caspio.com', '')
            auth_url = f"https://{account_id}.caspio.com/oauth/token"
            payload = {
                'grant_type': 'client_credentials',
                'client_id': self.caspio_config['client_id'],
                'client_secret': self.caspio_config['client_secret']
            }
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            resp = requests.post(auth_url, data=payload, headers=headers)
            if resp.status_code == 200:
                self.caspio_token = resp.json()['access_token']
                print("✓ Caspio authentication successful")
                return True
            else:
                print(f"✗ Caspio authentication failed: {resp.status_code}")
                print(f"Response: {resp.text}")
                return False
        except Exception as e:
            print(f"✗ Caspio authentication error: {e}")
            return False

    def read_google_sheet(self, sheet_url, worksheet_name=None):
        try:
            print(f"📋 Opening Google Sheet: {sheet_url}")
            if 'docs.google.com' in sheet_url:
                sheet = self.gc.open_by_url(sheet_url)
            else:
                sheet = self.gc.open_by_key(sheet_url)
            print(f"✓ Sheet opened successfully: {sheet.title}")

            # Chọn worksheet
            if worksheet_name:
                try:
                    ws = sheet.worksheet(worksheet_name)
                    print(f"✓ Found worksheet: {ws.title}")
                except:
                    ws = sheet.get_worksheet(0)
                    print(f"❌ Worksheet '{worksheet_name}' not found, using first worksheet: {ws.title}")
            else:
                ws = sheet.get_worksheet(0)
                print(f"✓ Using first worksheet: {ws.title}")

            all_values = ws.get_all_values()
            if not all_values:
                print("✗ No data found in sheet")
                return [], [], None

            headers = all_values[0]
            data_rows = []
            for idx, row in enumerate(all_values[1:], start=2):
                # pad với '' nếu thiếu cột
                while len(row) < len(headers):
                    row.append('')
                data_rows.append({
                    'row_number': idx,
                    'data': row
                })

            return data_rows, headers, None

        except Exception as e:
            print(f"✗ Error reading Google Sheet: {e}")
            return [], [], None

    def send_to_caspio(self, data_rows, field_mappings, headers):
        if not self.caspio_token:
            print("✗ No Caspio token available")
            return []

        account_id = self.caspio_config['account_id'].replace('https://', '').replace('http://', '')
        if account_id.endswith('.caspio.com'):
            account_id = account_id.replace('.caspio.com', '')
        api_url = f"https://{account_id}.caspio.com/rest/v2/tables/{self.caspio_config['table_name']}/records"

        headers_http = {
            'Authorization': f'Bearer {self.caspio_token}',
            'Content-Type': 'application/json'
        }

        print("DEBUG POST URL:", api_url)
        print("DEBUG TABLE NAME:", self.caspio_config['table_name'])
        print(f"\n🚀 Starting transfer of {len(data_rows)} records...")

        successes = []
        for row in data_rows:
            try:
                payload = {}
                for col_idx, field_name in field_mappings.items():
                    if col_idx < len(row['data']):
                        raw = row['data'][col_idx].strip()
                        if raw:
                            payload[field_name] = raw
                resp = requests.post(api_url, json=payload, headers=headers_http)
                if resp.status_code in (200, 201):
                    successes.append(row['row_number'])
                    print(f"   ✅ Row {row['row_number']}")
                else:
                    print(f"   ❌ Row {row['row_number']} - Status {resp.status_code}")
                    print(f"     {resp.text}")
                time.sleep(0.1)
            except Exception as e:
                print(f"   ❌ Error on row {row['row_number']}: {e}")

        return successes

    def transfer_data(self, sheet_url, worksheet_name, field_mappings):
        if not self.authenticate_google_sheets():
            return False
        if not self.get_caspio_token():
            return False

        data_rows, headers, _ = self.read_google_sheet(sheet_url, worksheet_name)
        if not data_rows:
            print("ℹ️ No rows to transfer")
            return True

        transferred = self.send_to_caspio(data_rows, field_mappings, headers)

        print("\n📈 FINAL TRANSFER SUMMARY")
        print("="*40)
        print(f"Total attempted: {len(data_rows)}")
        print(f"Successful: {len(transferred)}")
        print(f"Failed: {len(data_rows) - len(transferred)}")
        return True

def main():
    caspio_config = {
        'account_id': os.getenv('CASPIO_ACCOUNT_ID', 'your-account-id'),
        'client_id': os.getenv('CASPIO_CLIENT_ID', 'your-client-id'),
        'client_secret': os.getenv('CASPIO_CLIENT_SECRET', 'your-client-secret'),
        'table_name': os.getenv('CASPIO_TABLE_NAME', 'NganSachPT')
    }
    google_credentials_path = os.path.join(os.path.dirname(__file__), 'google-credentials.json')

    field_mappings = {
        0: 'Page_ID',
        1: 'Amount_Spent',
        2: 'Day',
        3: 'Reach',
        4: 'Impressions',
        5: 'Frequency',
        6: 'CPM_Cost_per_1000_Impressions',
        7: 'Link_Clicks',
        8: 'CPC_All',
        9: 'CTR_All',
        10: 'ChiNhanh'
    }

    sheet_url = os.getenv('SHEET_URL', 'https://docs.google.com/spreadsheets/...')
    worksheet_name = os.getenv('WORKSHEET_NAME', 'CPPhanTich')

    transfer = GoogleSheetsCaspioTransfer(caspio_config, google_credentials_path)
    transfer.transfer_data(sheet_url, worksheet_name, field_mappings)

if __name__ == "__main__":
    main()

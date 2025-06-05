import openpyxl
import requests
import os
import pandas as pd
import shutil
import datetime
from typing import Optional, List, Dict, Union
from io import BytesIO
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
 
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


class SharePointDownloader:
    
    def __init__(self, tenant_id, client_id, scope, key, 
                 site_name, document_library):

        self.tenant_id = tenant_id
        self.client_id = client_id
        self.scope = scope
        self.key = key
        self.site_name = site_name
        self.document_library = document_library
        
        self.base_api_url = 'https://graph.microsoft.com/v1.0'
        self.token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
        
        self.access_token = None
        self.headers = None
        self.site_id = None
        self.library_id = None
        self.file_index = {}
        
    def authenticate(self):
        try:
            token_data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': dbutils.secrets.get(scope=self.scope, key=self.key),
                'scope': 'https://graph.microsoft.com/.default'
            }
            
            response = requests.post(self.token_url, data=token_data)
            response.raise_for_status()
            
            self.access_token = response.json().get('access_token')
            if not self.access_token:
                print("Failed to obtain access token")
                return False
                
            self.headers = {'Authorization': f'Bearer {self.access_token}'}
            print("Authentication successful")
            return True
            
        except Exception as e:
            print(f"Authentication failed: {e}")
            return False
    
    def _get_site_id(self):
        try:
            site_resp = requests.get(
                f'{self.base_api_url}/sites/ukpowernetworks.sharepoint.com:/sites/{self.site_name}?$select=id',
                headers=self.headers
            )
            site_resp.raise_for_status()
            self.site_id = site_resp.json()['id']
            return True
        except Exception as e:
            print(f"Failed to get site ID: {e}")
            return False
    
    def _get_library_id(self):
        try:
            drives_resp = requests.get(
                f'{self.base_api_url}/sites/{self.site_id}/drives', 
                headers=self.headers
            )
            drives_resp.raise_for_status()
            
            drives = drives_resp.json()['value']
            self.library_id = next(
                (d['id'] for d in drives if d['name'] == self.document_library), 
                None
            )
            
            if not self.library_id:
                print(f"Document library '{self.document_library}' not found.")
                return False
            return True
            
        except Exception as e:
            print(f"Failed to get library ID: {e}")
            return False
    
    def discover_files(self, structured = True, folder_path = ""):
        if not self.headers:
            print("Not authenticated. Call authenticate() first.")
            return False
            
        if not self.site_id and not self._get_site_id():
            return False
            
        if not self.library_id and not self._get_library_id():
            return False
        
        try:
            if folder_path:
                folder_resp = requests.get(
                    f'{self.base_api_url}/drives/{self.library_id}/root:/{folder_path}',
                    headers=self.headers
                )
                if folder_resp.status_code != 200:
                    print(f"Folder not found: {folder_path}")
                    return False
                start_id = folder_resp.json()['id']
            else:
                root_resp = requests.get(
                    f'{self.base_api_url}/drives/{self.library_id}/root', 
                    headers=self.headers
                )
                root_resp.raise_for_status()
                start_id = root_resp.json()['id']
            
            self.file_index = {}
            
            self._recurse_items(start_id, structured=structured)
            
            print(f"Discovered {len(self.file_index)} files")
            return True
            
        except Exception as e:
            print(f"Failed to discover files: {e}")
            return False
    
    def _recurse_items(self, parent_id, current_path = '', structured = True):

        try:
            endpoint = f'{self.base_api_url}/drives/{self.library_id}/items/{parent_id}/children'
            resp = requests.get(endpoint, headers=self.headers)
            
            if resp.status_code != 200:
                print(f"Failed to get children for {parent_id}: {resp.status_code}")
                return
                
            for item in resp.json()['value']:
                name = item['name']
                item_id = item['id']
                
                if 'folder' in item:
                    new_path = os.path.join(current_path, name) if structured else name
                    self._recurse_items(item_id, new_path, structured)
                else:
                    if structured:
                        full_path = os.path.join(current_path, name) if current_path else name
                    else:
                        full_path = name
                    self.file_index[full_path] = item_id
                    
        except Exception as e:
            print(f"Error traversing folder {parent_id}: {e}")
    
    def download_file_content(self, item_id):

        try:
            url = f'{self.base_api_url}/drives/{self.library_id}/items/{item_id}/content'
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                return response.content
            else:
                print(f"Failed to download file - Status: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"Error downloading file: {e}")
            return None
    
    def compare_files(self, downloaded_content, target_file):

        if not os.path.exists(target_file):
            return True  # File doesn't exist, so it's "different"
        
        try:
            if target_file.endswith('.xlsx'):
                return self._compare_excel_files(downloaded_content, target_file)
            elif target_file.endswith('.csv'):
                return self._compare_csv_files(downloaded_content, target_file)
            elif target_file.endswith('.txt'):
                return self._compare_text_files(downloaded_content, target_file)
            else:
                print(f"Unsupported file type for comparison: {target_file}")
                return True
                
        except Exception as e:
            print(f"Error comparing files: {e}")
            return True
    
    def _compare_excel_files(self, downloaded_content, target_file):

        try:
            current_file = pd.read_excel(target_file)
            new_file = pd.read_excel(BytesIO(downloaded_content))
            return not current_file.equals(new_file)
        except Exception as e:
            print(f"Error comparing Excel files: {e}")
            return True
    
    def _compare_csv_files(self, downloaded_content, target_file):

        try:
            current_file = pd.read_csv(target_file)
            new_file = pd.read_csv(BytesIO(downloaded_content))
            return not current_file.equals(new_file)
        except Exception as e:
            print(f"Error comparing CSV files: {e}")
            return True
    
    def _compare_text_files(self, downloaded_content, target_file):

        try:
            with open(target_file, 'r', encoding='utf-8') as f:
                current_content = f.read()
            new_content = downloaded_content.decode('utf-8')
            return current_content != new_content
        except Exception as e:
            print(f"Error comparing text files: {e}")
            return True
    
    def create_version(self, target_file, versions_dir = None):

        try:
            if versions_dir is None:
                versions_dir = os.path.join(os.getcwd(), 'versions')
            
            os.makedirs(versions_dir, exist_ok=True)
            
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = os.path.basename(target_file)
            name, ext = os.path.splitext(filename)
            
            versioned_name = f"{name}_{timestamp}{ext}"
            versioned_path = os.path.join(versions_dir, versioned_name)
            
            shutil.copy2(target_file, versioned_path)
            print(f"Created version: {versioned_path}")
            return True
            
        except Exception as e:
            print(f"Error creating version: {e}")
            return False
    
    def download_single_file(self, file_path, output_dir, versioning_enabled = False, 
                           versions_dir = None):

        if file_path not in self.file_index:
            print(f"File not found in SharePoint: {file_path}")
            return False
        
        item_id = self.file_index[file_path]
        downloaded_content = self.download_file_content(item_id)
        
        if not downloaded_content:
            return False
        
        target_file = os.path.join(output_dir, file_path)
        
        # Check if file has changed
        if versioning_enabled == True:
            if os.path.exists(target_file):
                if not self.compare_files(downloaded_content, target_file):
                    print(f"File {file_path} is unchanged.")
                    return True
            
            # File has changed, create version if enabled
            if versioning_enabled:
                self.create_version(target_file, versions_dir)
        
        # Save the new file
        try:
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            with open(target_file, 'wb') as f:
                f.write(downloaded_content)
            print(f"Downloaded and saved: {target_file}")
            return True
            
        except Exception as e:
            print(f"Error saving file {target_file}: {e}")
            return False
    
    def download_all_files(self, output_dir, versioning_enabled = True,
                          structured = True, versions_dir = None,
                          folder_path = ""):

        if not self.file_index:
            # Auto-discover files from specified folder if not already done
            if not self.discover_files(structured=structured, folder_path=folder_path):
                print("No files discovered. Check folder path and permissions.")
                return False
        
        success_count = 0
        total_files = len(self.file_index)
        
        for file_path in self.file_index:
            if self.download_single_file(file_path, output_dir, versioning_enabled, versions_dir):
                success_count += 1
        
        print(f"Successfully downloaded {success_count}/{total_files} files")
        # return success_count == total_files
    
    def download_specific_files(self, file_list, output_dir,
                              versioning_enabled = False, 
                              versions_dir = None):

        if not self.file_index:
            print("No files discovered. Call discover_files() first.")
            return False
        
        success_count = 0
        
        for file_path in file_list:
            if self.download_single_file(file_path, output_dir, versioning_enabled, versions_dir):
                success_count += 1
        
        print(f"Successfully downloaded {success_count}/{len(file_list)} files")
        # return success_count == len(file_list)
    
    def list_files(self):
        return list(self.file_index.keys())
    
    def get_file_count(self):
        return len(self.file_index)
import requests
import json
import base64
import pandas as pd
from datetime import datetime
import streamlit as st

class GitHubStorage:
    def __init__(self, repo_owner, repo_name, github_token):
        self.repo_owner = repo_owner
        self.repo_name = repo_name
        self.github_token = github_token
        self.base_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents"
        self.headers = {
            "Authorization": f"token {github_token}",
            "Accept": "application/vnd.github.v3+json"
        }
    
    def file_exists(self, file_path):
        """Verifica se un file esiste nel repository"""
        try:
            response = requests.get(f"{self.base_url}/{file_path}", headers=self.headers)
            return response.status_code == 200
        except:
            return False
    
    def get_file_sha(self, file_path):
        """Ottiene l'SHA di un file esistente"""
        try:
            response = requests.get(f"{self.base_url}/{file_path}", headers=self.headers)
            if response.status_code == 200:
                return response.json()['sha']
        except:
            pass
        return None
    
    def save_dataframe(self, df, file_path, commit_message="Update data"):
        """Salva un DataFrame come CSV su GitHub"""
        try:
            # Converti DataFrame in CSV
            csv_content = df.to_csv(index=False)
            
            # Codifica in base64
            content_encoded = base64.b64encode(csv_content.encode()).decode()
            
            # Prepara i dati per l'API
            data = {
                "message": commit_message,
                "content": content_encoded
            }
            
            # Se il file esiste, aggiungi SHA per l'update
            if self.file_exists(file_path):
                sha = self.get_file_sha(file_path)
                if sha:
                    data["sha"] = sha
            
            # Invia richiesta
            response = requests.put(f"{self.base_url}/{file_path}", 
                                  headers=self.headers, 
                                  json=data)
            
            return response.status_code in [200, 201]
            
        except Exception as e:
            st.error(f"Errore nel salvataggio: {str(e)}")
            return False
    
    def load_dataframe(self, file_path):
        """Carica un DataFrame da CSV su GitHub"""
        try:
            response = requests.get(f"{self.base_url}/{file_path}", headers=self.headers)
            
            if response.status_code == 200:
                # Decodifica il contenuto
                content = response.json()['content']
                csv_content = base64.b64decode(content).decode()
                
                # Converti in DataFrame
                from io import StringIO
                df = pd.read_csv(StringIO(csv_content))
                
                # Riconverti le date
                if 'data' in df.columns:
                    df['data'] = pd.to_datetime(df['data'], errors='coerce')
                if 'import_datetime' in df.columns:
                    df['import_datetime'] = pd.to_datetime(df['import_datetime'], errors='coerce')
                
                return df
            else:
                return pd.DataFrame()
                
        except Exception as e:
            st.error(f"Errore nel caricamento: {str(e)}")
            return pd.DataFrame()
    
    def save_metadata(self, metadata, file_path="data/system_metadata.json"):
        """Salva i metadati come JSON"""
        try:
            # Aggiungi timestamp
            metadata['last_update'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Converti in JSON
            json_content = json.dumps(metadata, indent=2)
            
            # Codifica in base64
            content_encoded = base64.b64encode(json_content.encode()).decode()
            
            data = {
                "message": "Update metadata",
                "content": content_encoded
            }
            
            # Se il file esiste, aggiungi SHA
            if self.file_exists(file_path):
                sha = self.get_file_sha(file_path)
                if sha:
                    data["sha"] = sha
            
            response = requests.put(f"{self.base_url}/{file_path}", 
                                  headers=self.headers, 
                                  json=data)
            
            return response.status_code in [200, 201]
            
        except Exception as e:
            st.error(f"Errore nel salvataggio metadati: {str(e)}")
            return False
    
    def load_metadata(self, file_path="data/system_metadata.json"):
        """Carica i metadati da JSON"""
        try:
            response = requests.get(f"{self.base_url}/{file_path}", headers=self.headers)
            
            if response.status_code == 200:
                content = response.json()['content']
                json_content = base64.b64decode(content).decode()
                return json.loads(json_content)
            else:
                # Metadati di default
                return {
                    'import_history': [],
                    'last_update': None,
                    'categories': ['Software', 'Domini e Hosting', 'Servizi Web', 'Abbonamenti', 'Hardware', 'Consulenze', 'Marketing', 'Altro'],
                    'subcategories': {
                        'Software': ['Licenze', 'Abbonamenti SaaS', 'Plugin', 'Applicazioni'],
                        'Domini e Hosting': ['Registrazione Domini', 'Hosting Web', 'SSL', 'CDN'],
                        'Servizi Web': ['API', 'Cloud Storage', 'Email Service', 'Backup'],
                        'Marketing': ['Advertising', 'Social Media', 'Email Marketing', 'SEO Tools']
                    }
                }
                
        except Exception as e:
            st.error(f"Errore nel caricamento metadati: {str(e)}")
            return {}

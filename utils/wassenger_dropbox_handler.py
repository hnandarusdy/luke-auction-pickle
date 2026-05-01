import dropbox
from helper import CONFIG, DROPBOX_TOKEN, logger
import json 
import os 
import requests

from dropbox.dropbox_client import BadInputException
from test_refresh_dropbox import refresh_access_token as do_refresh

# Replace with your Dropbox access token
DROPBOX_ACCESS_TOKEN = "sl.u.AF0q1sTVUpbWvACwUbJQTgooPrzMqS3cECsEGqVI7_IfQAQvtQk0Jrw2AV5hnW0YZn2B1f3uj-EXZiirVjBoHSoMOF1SUVX5V29dEnHzyb1GMnJdhSGVxdtSf3MLDlKn9lHKuwYJgUFktGTzzwawzdAK5Jyqen9mP82H4dioqa4ulEvZf0fwrVFKzs8MefNHy4tjlIwaCLJUviltlcY1G68rPiPdYz5Xb0D5sU25q3__ZSAMu27tqhD4aDwNOJwUMATwCsY7m9cKJ2KhP0vJ7ryh-L2tJcxI9gEgjcuj9iEbu7yDhyKUvq_4Vx8zZH9pK81UFNzXx4UfZcu6FlQlnMJLf1pRlNtB9jFKg-iSfVzfLAqFbcW85LUv0ERGvUnQ50ziEv0lLnAkLFXtoNq6EUJoDfFwSyRXPB_qvSe3NS5jFhZDaZ6kuGolCal0p1O6-ICmb6P-dbhVv6k0XqLm-c1wThJ7YXRuQc8RcBj4fwt55enlu8rVhzGnXKQ4J1E2j_yfVQAn5X_S6_2sxhm45dokO80VbQGQFL9JBUkVGBCW--SVCERxny-BCGEsTmewmBUiLQpSE9K8mTNn8wYsYnDuAKgKPzYgJRIfUVjzqUPpPYN0V7QKlB1eemMo_JEMIr5Qh7klCjiRYWYJS292MsS8x9aO6UG8Ar-b4ly3YmcZt4AYaEcnrYuslRfZSpA_R-4nwteiUIGbs-6EYlwHTX79qNk2IUSp4m5rhhZFY9D8D2kxOwGbdXCBJlfNICK3iwkoZT-tlo5Q_UMtTDBbtgHC08Ys_QRr9dWbMXzH7lrjcIpZquMu_GaSvQd6fciFdBqwK7-VoI6GElSKhKgF_zrJqx-WN5yZxua-_0TxoIdnTyo6cOcgIptPAZnKx8I71rGarTCHZ8O46wSCb-VBMjHrE0EuvWV99thO56NAmsVsLNLPiPE6esn1S50nVH1GWIDec9bVi8e1iMXJha1pruzvf2znUylVyEkiAzg07dpEPPUY8d9qn6gVWYZeHs07VhMnQUB5P7GFkSg1aNkYC2PlIMaCG9Gp2Hxomi629veVCDtR1wrzIECuoy1SJ3gDHEIq4dhv4GQimCefb0DrBoCfPE-e6BtfZMV7UgflHGQyEw"
DROPBOX_ACCESS_TOKEN = DROPBOX_TOKEN['access_token'] if DROPBOX_TOKEN else DROPBOX_ACCESS_TOKEN
DROPBOX_ACCESS_TOKEN = ""
# folder_name = "/test_folder1"

# # Connect to Dropbox
# dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)

# try:
#     # Create the folder (ignore if already exists)
#     folder_metadata = dbx.files_create_folder_v2(folder_name)
#     print(f"Folder created: {folder_metadata.metadata.path_display}")
# except dropbox.exceptions.ApiError as e:
#     # If folder already exists, fetch its metadata
#     if (hasattr(e.error, 'is_path') and e.error.is_path() and
#         hasattr(e.error.get_path(), 'is_conflict') and e.error.get_path().is_conflict()):
#         folder_metadata = dbx.files_get_metadata(folder_name)
#         print(f"Folder already exists: {folder_metadata.path_display}")
#     else:
#         raise e

# # Create a shared link for the folder
# shared_link_metadata = dbx.sharing_create_shared_link_with_settings(folder_name)
# print("Dropbox Folder URL:", shared_link_metadata.url)




class WassengerDropboxHandler:
    def __init__(self, access_token=None):
        self.app_key = CONFIG['dropbox']['prod']['app_key']
        self.app_secret = CONFIG['dropbox']['prod']['app_secret']
        self.token = CONFIG['dropbox']['prod']['dropbox_token_file']
        self.access_token = access_token
        self.member_id = CONFIG['dropbox']['prod']['member_id']
        self.namespace_id = CONFIG['dropbox']['prod']['namespace_id']

        self.team_dbx = dropbox.DropboxTeam(self.access_token)
        self.user_dbx = self.team_dbx.as_user(self.member_id)

        user_dbx_team_space = self.user_dbx.with_path_root(dropbox.common.PathRoot.namespace_id(self.namespace_id))
        self.dbx = user_dbx_team_space
        

    def create_and_share_folder(self, folder_name):
        # Create folder if not exists
        try:
            folder_metadata = self.dbx.files_create_folder_v2(folder_name)
            print(f"Folder created: {folder_metadata.metadata.path_display}")
        except dropbox.exceptions.ApiError as e:
            if (hasattr(e.error, 'is_path') and e.error.is_path() and
                hasattr(e.error.get_path(), 'is_conflict') and e.error.get_path().is_conflict()):
                folder_metadata = self.dbx.files_get_metadata(folder_name)
                print(f"Folder already exists: {folder_metadata.path_display}")
            else:
                raise e

        # Create a shared link for the folder and return the URL
        shared_link_metadata = self.dbx.sharing_create_shared_link_with_settings(folder_name)
        # Prepare result dict
        result = {
            'path_display': folder_metadata.metadata.path_display if hasattr(folder_metadata, 'metadata') else folder_metadata.path_display,
            'id': folder_metadata.metadata.id if hasattr(folder_metadata, 'metadata') else folder_metadata.id,
            'shared_url': shared_link_metadata.url
        }
        return result
    
    def find_folder_by_name(self, folder_name):
        """
        Search for a folder by its path or name. Returns (metadata, shared_url) if found, else (None, None).
        """
        # Use correct SearchOptions class
        result = self.dbx.files_search_v2(query=folder_name, options=dropbox.files.SearchOptions(filename_only=True, file_status=dropbox.files.FileStatus.active))
        matches = result.matches if hasattr(result, 'matches') else []
        for match in matches:
            metadata = match.metadata.get_metadata()
            if metadata.path_display == folder_name and metadata['.tag'] == 'folder':
                # Try to get or create a shared link
                try:
                    links = self.dbx.sharing_list_shared_links(path=folder_name, direct_only=True).links
                    if links:
                        shared_url = links[0].url
                    else:
                        shared_url = self.dbx.sharing_create_shared_link_with_settings(folder_name).url
                except Exception:
                    shared_url = None
                return metadata, shared_url
        return None, None
    
    def list_all_folders(self, root_path=""): 
        """
        List all folders in Dropbox starting from root_path (default is root). Returns a list of folder paths.
        """
        folders = []
        queue = [root_path]
        while queue:
            current_path = queue.pop(0)
            try:
                result = self.dbx.files_list_folder(current_path)
                for entry in result.entries:
                    if isinstance(entry, dropbox.files.FolderMetadata):
                        folders.append(entry.path_display)
                        queue.append(entry.path_display)
                while result.has_more:
                    result = self.dbx.files_list_folder_continue(result.cursor)
                    for entry in result.entries:
                        if isinstance(entry, dropbox.files.FolderMetadata):
                            folders.append(entry.path_display)
                            queue.append(entry.path_display)
            except Exception:
                continue
        return folders
    
    def get_folder_by_path(self, folder_path): #/ example: "/Full Wassenger Images Dev/subfolder3"
        """
        Get folder metadata and shared URL by exact path.
        """
        try:
            metadata = self.dbx.files_get_metadata(folder_path)
            if isinstance(metadata, dropbox.files.FolderMetadata):
                # Try to get or create a shared link
                try:
                    links = self.dbx.sharing_list_shared_links(path=folder_path, direct_only=True).links
                    if links:
                        shared_url = links[0].url
                    else:
                        shared_url = self.dbx.sharing_create_shared_link_with_settings(folder_path).url
                except Exception:
                    shared_url = None
                return metadata, shared_url
        except Exception:
            return None, None
        
    def save_tokens(self, tokens):
        
        with open(self.token, 'w') as f:
            json.dump(tokens, f, indent=2)
        logger.info(f"Tokens saved to {self.token}")


    def load_refresh_token(self):
        if self.token and os.path.exists(self.token):
            with open(self.token, 'r') as f:
                data = json.load(f)
                return data.get('refresh_token')
        return None
    
    def refresh_access_token(self):
        refresh_token = self.load_refresh_token()
        if not refresh_token:
            print("No refresh token found. Run the OAuth flow first.")
            return None
        response = requests.post(
            'https://api.dropbox.com/oauth2/token',
            data={
                'grant_type': 'refresh_token',
                'refresh_token': refresh_token,
            },
            auth=(self.app_key, self.app_secret)
        )
        tokens = response.json()
        if 'access_token' in tokens:
            tokens['refresh_token'] = refresh_token  # keep the same refresh token
            self.save_tokens(tokens)
            print("New access token:", tokens.get('access_token'))
            return tokens.get('access_token')
        else:
            print("Failed to refresh token:", tokens)
            return None
    
    def upload_file_to_folder_id(self, folder_id, file_path, dropbox_filename=None):
        """
        Upload a file to a Dropbox folder using the folder's ID.
        :param folder_id: The Dropbox folder ID (e.g., 'id:xxxxxxx')
        :param file_path: Local path to the file to upload
        :param dropbox_filename: Optional filename to use in Dropbox (default: same as local file)
        :return: File metadata if successful, else None
        """
        if not dropbox_filename:
            dropbox_filename = os.path.basename(file_path)
        # Dropbox API path format for folder ID: 'id:xxxxxxx/filename.ext'
        dropbox_path = f"{folder_id}/{dropbox_filename}"
        try:
            with open(file_path, 'rb') as f:
                file_metadata = self.dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)
            logger.info(f"Uploaded {file_path} to Dropbox folder {folder_id} as {dropbox_filename}")
            # Prepare custom metadata to return
            result = {
                'image_uploaded_timestamp': file_metadata.server_modified.isoformat() if hasattr(file_metadata, 'server_modified') else None,
                'img_id': getattr(file_metadata, 'id', None),
                'path_display': getattr(file_metadata, 'path_display', None),
                'folder_id': folder_id,
                'size': getattr(file_metadata, 'size', None)
            }
            return result
        except Exception as e:
            logger.error(f"Failed to upload {file_path} to Dropbox: {e}")
            return None




if __name__ == "__main__":


    dropbox_handler = WassengerDropboxHandler()
    folder_path = f'/Radnet_Pictures/WA_Uploads/{folder_name}'

    
    result = dropbox_handler.create_and_share_folder(folder_path)
    print("Folder Created and Shared:")
    print(f"Path: {result['path_display']}")
    print(f"Shared URL: {result['shared_url']}")
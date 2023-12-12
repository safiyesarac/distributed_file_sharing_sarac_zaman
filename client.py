import requests

# Define the API endpoints
UPLOAD_ENDPOINT = 'http://128.214.9.98:9000/upload'
DOWNLOAD_ENDPOINT = 'http://128.214.9.98:9000/download'
LIST_ENDPOINT = 'http://128.214.9.98:9000/list'
# Test file upload
def test_upload():
    files = {'file': open('test_file_.txt', 'rb')}
    response = requests.post(UPLOAD_ENDPOINT, files=files)
    assert response.status_code == 200
    print("Upload test passed")

# Test file download
def test_download():
    response = requests.get(DOWNLOAD_ENDPOINT, params={'filename': 'test_file_2.txt'})
    assert response.status_code == 200
    print("Download test passed")

# Test file listing
def test_list_files():
    
    response = requests.get(LIST_ENDPOINT)
    response_data = response.json()
    print("List files response:", response_data)

    # Check if 'test_file.txt' is in any of the lists in the response
    file_found = any('test_file.txt' in files for files in response_data.values())
    assert file_found, f"'test_file.txt' not found in response: {response_data}"
    print("List files test passed")


## Run the tests
test_upload()

test_list_files()

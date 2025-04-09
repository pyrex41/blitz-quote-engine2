import gdown

url = "https://drive.google.com/file/d/1DkV56x7hv8U7htBrE-Etl2TYlTFebfP_/view?usp=sharing"
output = "downloaded_file"

# Convert the sharing URL to direct download format
file_id = url.split("/")[-2]
direct_url = f"https://drive.google.com/uc?id={file_id}"

# Download the file
gdown.download(direct_url, output, quiet=False)
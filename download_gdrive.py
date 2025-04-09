
import gdown
import argparse

def download_from_gdrive(url=None, file_id=None, output="downloaded_file"):
    if not url and not file_id:
        raise ValueError("Either URL or file ID must be provided")
    
    if url:
        # Convert the sharing URL to direct download format
        file_id = url.split("/")[-2]
        
    # Create direct download URL
    direct_url = f"https://drive.google.com/uc?id={file_id}"
    
    # Download the file
    gdown.download(direct_url, output, quiet=False)

def main():
    parser = argparse.ArgumentParser(description="Download files from Google Drive")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-u", "--url", help="Google Drive sharing URL")
    group.add_argument("-i", "--id", help="Google Drive file ID")
    parser.add_argument("-o", "--output", default="downloaded_file", help="Output filename")
    
    args = parser.parse_args()
    download_from_gdrive(url=args.url, file_id=args.id, output=args.output)

if __name__ == "__main__":
    main()

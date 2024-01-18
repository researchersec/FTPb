import hashlib
from ftplib import FTP
import os
import smtplib
from concurrent.futures import ThreadPoolExecutor
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config import FTP_HOST, FTP_USER, FTP_PASSWORD, REMOTE_DIRECTORY, LOCAL_BACKUP_DIRECTORY, \
    SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, TO_EMAIL, NUM_THREADS

def calculate_file_hash(file_path, hash_algorithm="md5"):
    """Calculate the hash of a file."""
    hash_object = hashlib.new(hash_algorithm)
    with open(file_path, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            hash_object.update(byte_block)
    return hash_object.hexdigest()

def send_email(subject, body, to_email, smtp_server, smtp_port, smtp_user, smtp_password):
    msg = MIMEMultipart()
    msg['From'] = smtp_user
    msg['To'] = to_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, to_email, msg.as_string())

def process_file(file_name, remote_directory, local_backup_directory, ftp):
    remote_file_path = os.path.join(remote_directory, file_name)
    local_file_path = os.path.join(local_backup_directory, file_name)

    with open(local_file_path, 'wb') as local_file:
        ftp.retrbinary(f"RETR {file_name}", local_file.write)

    # Calculate hash for both remote and local files
    remote_file_hash = ftp.sendcmd(f"MD5 {file_name}").split(' ')[-1].strip()
    local_file_hash = calculate_file_hash(local_file_path)

    # Verify if the hash values match
    if remote_file_hash != local_file_hash:
        raise Exception(f"Verification failed for file: {file_name}")

def backup_ftp_files(ftp_host, ftp_user, ftp_password, remote_directory, local_backup_directory,
                     smtp_server, smtp_port, smtp_user, smtp_password, to_email, num_threads):
    try:
        # Connect to FTP server
        ftp = FTP(ftp_host)
        ftp.login(ftp_user, ftp_password)

        # Change to the remote directory
        ftp.cwd(remote_directory)

        # List all files in the remote directory
        file_list = ftp.nlst()

        # Create local backup directory if it doesn't exist
        if not os.path.exists(local_backup_directory):
            os.makedirs(local_backup_directory)

        # Process files using multithreading
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = []
            for file_name in file_list:
                future = executor.submit(process_file, file_name, remote_directory, local_backup_directory, ftp)
                futures.append(future)

            # Wait for all threads to complete
            for future in futures:
                future.result()

        # Close the FTP connection
        ftp.quit()

        # Send success email
        subject = "FTP Backup Successful"
        body = "The backup process completed successfully."
        send_email(subject, body, to_email, smtp_server, smtp_port, smtp_user, smtp_password)

    except Exception as e:
        # Send error email
        subject = "FTP Backup Failed"
        body = f"The backup process encountered an error:\n\n{str(e)}"
        send_email(subject, body, to_email, smtp_server, smtp_port, smtp_user, smtp_password)

if __name__ == "__main__":
    backup_ftp_files(FTP_HOST, FTP_USER, FTP_PASSWORD, REMOTE_DIRECTORY, LOCAL_BACKUP_DIRECTORY,
                     SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, TO_EMAIL, NUM_THREADS)

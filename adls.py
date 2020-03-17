from azure.datalake.store import core, lib, multithread
from os import path
import pathlib
import json


#A class that represents login credentials for Azure Data Lake Storage Gen 1 access
class ADLSCredential:

    def loads(self, env_name):
        """
        Load the json file which contains the credential to access ADLS
        """
        if env_name is None or type(env_name) is not str or len(env_name) == 0:
            raise Exception("env_name is invalid type")

        env_filename = env_name if env_name.endswith(".env.json") else env_name + ".env.json"

        if path.exists(env_filename):
            with open(env_filename, 'r') as f:
                self.__dict__ = json.load(f)
        else:
            raise Exception("File not found error")

    def is_valid(self):
        valid = True
        for field in self.__dict__:
            if len(self.__dict__[field]) == 0:
                valid = False
        return valid
        

class ADLSBackup:
    """
    A class that backups file to the Azure Datalake Storage Gen 1
    """

    def __init__(self, adlscred):
        """
        Initialization argument adlscred only accept object of type ADLSCredential
        """
        if not isinstance(adlscred, ADLSCredential):
            raise Exception("adlscred is not of type ADLSCredential")

        if not hasattr(adlscred, "is_valid"):
            raise Exception("adlscred has no function named is_valid")

        if not adlscred.is_valid():
            raise Exception("adlscred is not valid, one more fields is missing")

        auth = lib.auth(tenant_id=adlscred.tenant_id, 
                        username=adlscred.username,
                        password=adlscred.password, 
                        resource='https://datalake.azure.net/')

        self.client = core.AzureDLFileSystem(auth, 
                                             store_name=adlscred.adls_account_name)

    def make_folder(self, folderpath):
        if folderpath is None:
            raise Exception("folderpath is nil")
        if not isinstance(folderpath, pathlib.Path):
            raise Exception("folderpath is not of type Path")

        if self.client.exists(str(folderpath)):
            print("check folder....found!")
        else:
            self.client.mkdir(str(folderpath))
            print("created folder path " + str(folderpath) + "!")

    def in_progress(self, current, total):
        """
        A call back method to report the progress of the file in transfer
        param: current: the number of bytes transferred so far, 
               total: the number of bytes to be transferred.
        """
        if current != total:
            print("Transferred {current} bytes so far".format(current))
        else:
            print("Transfer complete!")

    def __pass_final_check(self, localpath, remotepath, filesize):
        """
        Final check if file has been uploaded to ADLS Gen1
        local_size: file size of local file in bytes
        return: boolean
        """
        adls_file_path = str(remotepath.joinpath(localpath.name))

        return self.client.exists(adls_file_path)

    def __is_valid_path(self, path):
        if path is None:
            return False
        elif not isinstance(path, pathlib.Path):
            return False
        else:
            return True

    def has_remote_folder(self, folderpath):
        return self.client.exists(str(folderpath))

    def transfer(self, local_filepath, remote_folderpath):
        """
        Transfer a file by the file name to Azure Datalake Storage instance.
        Supports single file upload
        param: local_filepath: Path of the file on the local machine, datatype is Path
               remote_filepath: Path of the file on the remote machine, datatype is Path
        """
        
        if not self.__is_valid_path(local_filepath):
            raise Exception("local_filepath is not valid")

        if not local_filepath.exists():
            raise Exception(local_filepath.name + " not exists at path " + str(local_filepath))

        if not self.__is_valid_path(remote_folderpath):
            raise Exception("remote_filepath is not valid")

        if not self.has_remote_folder(remote_folderpath):
            self.make_folder(remote_folderpath)
            
        remote_filepath = remote_folderpath.joinpath(local_filepath.name)    
        
        if self.client.exists(str(remote_filepath)):
            print("{filename} already exists in datalake!".format(filename=local_filepath.name))

        else: 
            local_file_size = path.getsize(str(local_filepath))
            should_overwrite = True

            multithread.ADLUploader(self.client, 
                                    lpath=str(local_filepath), 
                                    rpath=str(remote_filepath), 
                                    overwrite=should_overwrite, 
                                    progress_callback=self.in_progress, 
                                    buffersize=local_file_size, 
                                    blocksize=local_file_size)

            data = {"folder":remote_folderpath, "file":local_filepath.name, "size":local_file_size}

            if self.__pass_final_check(local_filepath, remote_folderpath, local_file_size):
                print("Finished transferring {filename}".format(filename=local_filepath.name))

# Introduction

 **adls** is designed to be a simple python module for file transfer from a Linux machine to Azure DataLake Storage Gen1.   


## Setting credentials
The first step in providing adls access to your Azure DataLake Storage Gen1 instance is to modify the **adls.env.json** file.  This file contains all the fields necessary for adls to access and interact with your ADLS instance.

ADLSCredential is the class that represents the credentials to interact with ADLS

First import the class

    from adls import ADLSCredential

Next, give the name of the json file 

    adlscred = ADLSCredentials()
    adlscred.loads("adls")
	#or
	adlscred.loads("adls.env.json")

Here we see that **adls** is the name of the json file, you could your settings file anything name you want.  It will automatically look for \<name>.env.json file within the same directory.  

## Third party libraries

This module depends on the following external packages

[azure.data.lake.store](https://docs.microsoft.com/en-us/python/api/azure-datalake-store/azure.datalake.store?view=azure-python)

    pip install azure-datalake-store
    from azure.datalke.store import core, lib, multithread 

json

    import json

pathlib

    import pathlib

path

    from os import path



	
	
## Interact with Azure DataLake Storage Gen1

After setting up the credential, we can now interact with ADLS.

First import the class

    from adls import ADLSBackup
 
We can now create an instance of the class to interact ADLS.

    adls_backup = ADLSBackup(adlscred)
    adls_backup.transfer(local_filepath, remote_folderpath)
    ## local_file: i.e /home/user/backup.sql
    ## remote_folderpath: i.e /sampledirectory

After we pass in the credential to initialize an instance of ADLSBackup, we can then implement the transfer method in this object.  **local_filepath** is the path of the file on the local machine that we want to transfer.  **remote_folderpath** is the folder path of where we want the file to transfer into.

**Note:** Both local_filepath and remote_folderpath should be of type **Path** from the pathlib library in Python.

In the above example, the directory **/sampledirectory** may or may not be created on the ADLS instance at the time when the program runs.  If the directory is not created, the program will automatically create the folder before transferring the file.








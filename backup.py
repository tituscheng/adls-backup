from adls import ADLSCredential, ADLSBackup
import sys
from pathlib import Path

ADLS_ENV_NAME = "adls"

if len(sys.argv) == 3:
    local = sys.argv[1]
    remote = sys.argv[2]
    adlscred = ADLSCredential()
    adlscred.loads(ADLS_ENV_NAME)
    backup = ADLSBackup(adlscred)
#        backup.make_folder("sampledirectory")
    backup.transfer(Path(local), Path(remote))
else:
    print("Filename not found")

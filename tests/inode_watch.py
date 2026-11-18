"""Sample owned inode use without reading payload bytes."""
import json
import os
from pathlib import Path
import socket
import sys
import time

root=Path(sys.argv[1]);host=socket.gethostname();output=root/('inodes-'+host+'.json')
result={'host':host,'start_utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'interval_s':2,'samples':[]}
paths=list(root.glob('node-*/result.json'))
work=None
for path in paths:
    data=json.loads(path.read_bytes())
    if data.get('host')==host:work=Path(data['work'])
assert work is not None
while work.exists():
    seen=set();allocated=0;files=0;directories=0
    for path in [work,*work.rglob('*')]:
        try:
            status=path.lstat()
            inode=(status.st_dev,status.st_ino)
            if inode in seen:continue
            seen.add(inode);allocated+=status.st_blocks*512
            if path.is_dir():directories+=1
            else:files+=1
        except FileNotFoundError:pass
    result['samples'].append({'utc':time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),'owned_inodes':len(seen),'files':files,'directories':directories,'allocated_bytes':allocated})
    temporary=output.with_suffix('.tmp');temporary.write_text(json.dumps(result,indent=2));os.replace(temporary,output)
    time.sleep(2)
result['end_utc']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
output.write_text(json.dumps(result,indent=2))

#该脚本实现的功能是将前一天的逻辑备份归档到远程备份机器上
import datetime
from pathlib import Path
from mtk.utils.cmd import exec_cmd
from mtk.alert import sender
import shutil

today = datetime.date.today()
NFSHOST = 'mysqlnfs.produce.zs'
yesterday = today - datetime.timedelta(1)
local_dir = Path("/data0/backup/sqldump") / "{}".format(yesterday)
arch_dir = Path("/data1/mysql/sqldump")


# 每天执行，将前一天的sqldump拷贝到归档机器上
def arch_dump():
    cmd = "ssh {}".format(NFSHOST) + ' "/bin/mkdir -p {}" '.format(arch_dir)
    rc, stdout, stderr = exec_cmd(cmd)
    if rc != 0:
        sender.send_all("mkdir {} failed . stderr : {}".format(arch_dir, stderr))
        return
    cmd = "rsync -avzP --bwlimit=5000 {} {}:{}".format(local_dir, NFSHOST, arch_dir)
    rc, stdout, stderr = exec_cmd(cmd)
    if rc != 0:
        sender.send_all("{} rsync is failed .stderr : {} ".format(local_dir, stderr))
        return
    shutil.rmtree(local_dir)


arch_dump()

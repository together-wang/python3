#该脚本实现的功能是每周将上一周的xtrabackup备份，远程归档到备份机器上
import datetime
from mtk.mysql.backup import NxinXtraBackupDir, DATE_FORMAT
from mtk.mysql.runtime import get_mysql_running_ports
from pathlib import Path
from mtk.utils.cmd import exec_cmd
from mtk.alert import sender
import shutil

today = datetime.date.today()
NFSHOST = 'mysql-jhl01.dev.zs'
fina_ports = [13306,13307,13308,13309,13310]

#该函数的作用是根据日期，找到上周起始位置和终点位置；即上周日-周六
def last_week(date):
    weekday = date.weekday()
    if weekday == 6:
        begin = date - datetime.timedelta(7)
        end = date - datetime.timedelta(1)
    else:
        begin = date - datetime.timedelta(weekday + 1) - datetime.timedelta(7)
        end = date + datetime.timedelta(5 - weekday) - datetime.timedelta(7)
    return begin, end

#定义本地目录，归档机器目录，因业务区分金融与非金融业务，因此归档机器目录分为两个
last_sunday, this_saturday = last_week(today)
no_fina_path = Path('/arch/mysql/xtrabackup') / "{}_{}".format(last_sunday, this_saturday)
fina_path = Path('/arch/mysql/xtrabackup_finance') / "{}_{}".format(last_sunday, this_saturday)
local_path = Path('/data0/backup/xtrabackup') / "{}_{}".format(last_sunday, this_saturday)

def arch_xtra():
    cmd = "ssh {} ".format(NFSHOST) + \
          ' "/bin/mkdir -p {} && /bin/mkdir -p {}" '.format(no_fina_path, fina_path)
    rc, stdout, stderr = exec_cmd(cmd)
    if rc != 0:
        #sender.send_all("mkdir {} or {} failed . stderr : {}".format(no_fina_path, fina_path, stderr))
        print("mkdir {} or {} failed . stderr : {}".format(no_fina_path, fina_path, stderr))
        return
#get_mysql_running_ports是一个函数，能够找到存活的MySQL端口
    for port in get_mysql_running_ports():
        nxbd = local_path / str(port)
        if not nxbd.exists():
            #sender.send_all("{} is not exists,maybe this port is new instance".format(nxbd))
            print("{} is not exists,maybe this port is new instance".format(nxbd))
            continue

        # 判断端口是否是金融库的端口
        if port in fina_ports:
            copy_cmd = "rsync -avzP --bwlimit=5000 {} {}:{}".format(nxbd, NFSHOST, fina_path)
        else:
            copy_cmd = "rsync -avzP --bwlimit=5000 {} {}:{}".format(nxbd, NFSHOST, no_fina_path)

        rc, stdout, stderr = exec_cmd(copy_cmd)
        if rc != 0:
            #sender.send_all("cmd: {} is failed . stderr : {} ".format(cmd, stderr))
            print("cmd: {} is failed . stderr : {} ".format(cmd, stderr))
            continue
        shutil.rmtree(nxbd)   #拷贝成功后，需要删除该目录
    cmd = "rmdir {}".format(local_path)   #如果上一级目录不为空，说明拷贝过程中有出现失败的。需要打印错误
    rc, stdout, stderr = exec_cmd(cmd)
    if rc != 0:
        #sender.send_all("rmdir is failed. dir: {},stderr: {}".format(local_path, stderr))
        print("rmdir is failed. dir: {},stderr: {}".format(local_path, stderr))


arch_xtra()

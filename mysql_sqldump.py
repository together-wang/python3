import datetime
import socket
from pathlib import Path
from mtk.mysql.backup import writable, financial_ports
from mtk.utils.cmd import exec_cmd
from mtk.alert import sender
from mtk.mysql.runtime import get_mysql_running_ports


today = datetime.date.today()
hostname = socket.gethostname()
mydumper = Path("/usr/local/bin/mydumper")
backup_base = Path("/data0/backup/sqldump")
backup_today = backup_base / '{}'.format(today)
one_clock = datetime.datetime.today().replace(hour=1, minute=0, second=0)
seven_clock = datetime.datetime.today().replace(hour=7, minute=0, second=0)


def backup(exclude_ports: list):
    # 检查二进制文件
    if not mydumper.exists():
        sender.send_all("mysql_sqldump.py, mydumper binary file is not exist. err: {}".format(mydumper))
        return

    # 检查备份目录是否可写
    if not writable(backup_base):
        sender.send_all("mysql_sqldump.py, err: {} not writable".format(backup_base))
        return

    # 判断是否存在备份
    if not backup_base.exists():
        backup_base.mkdir(parents=True)

    # 判断是否存在备份目录
    if not backup_today.exists():
        backup_today.mkdir(parents=True)

    running_ports = get_mysql_running_ports()
    for port in running_ports:
        # 排除的端口不做备份
        if port in exclude_ports:
            continue

        # 如果没有端口的目录说明没有进行过备份
        # 这里需要做一次备份
        base_port_dir = backup_today / "{}_{}".format(hostname, port)
        if not base_port_dir.exists():
            sqldump(port)

    # 金融库进行额外的备份
    extra_backup(running_ports)


def extra_backup(running_ports):
    # 金融库凌晨时间不做备份
    if one_clock < datetime.datetime.now() < seven_clock:
        return

    for port in financial_ports:
        if port in running_ports:
            sqldump(port)


def sqldump(port):
    base_port_dir = backup_today / "{}_{}".format(hostname, port)
    cmd = "{} -u root  -S /tmp/mysql_{}.sock " \
          "--regex '^(?!(mysql|test|information_schema|performance_schema|sys))' " \
          "-o {} --triggers --events  --routines -c".\
        format(mydumper, port, base_port_dir)
    rc, stdout, stderr = exec_cmd(cmd)
    if rc != 0:
        sender.send_all("mysql_sqldump.py, sqldump is failed. port {} . error: {}".format(port, stderr))


if __name__ == '__main__':
    backup(exclude_ports=[5004, 6004])

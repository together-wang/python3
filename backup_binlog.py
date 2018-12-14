# -*- coding: utf-8 -*-
# 该脚本的作用是为了备份金融端口数据库每天的binlog
import datetime
from pathlib import Path
from mtk.alert import sender
from mtk.utils.cmd import exec_cmd
from mtk.mysql.backup import financial_ports
from mtk.mysql.runtime import get_mysql_running_ports, get_mysql_runtime_instances

instances = get_mysql_runtime_instances()
today = datetime.date.today()
NFSHOST = 'mysqlnfs.produce.zs'
archive_base_dir = Path('/data1/mysqlbinlog')
archive_today_dir = archive_base_dir / "{}".format(today)


# 得到该机器上的金融端口实例列表，（金融端口数据库在5.6和5.7均存在）
def running_ports():
    empty_port = []
    for port in financial_ports:
        if port in get_mysql_running_ports():
            empty_port.append(port)
    return empty_port


# 得到binlog.index文件中的最后一个binlog文件名
def tail_n(file, n):
    with open(file) as f:
        x = f.readlines()
        return x[-n].strip()


# 得到datadir、log-bin、以及全部binlog
def dir_bin_all_binlog(port):
    i = instances[port]
    data_dir = i.config['datadir']
    if "log_bin" in i.config:
        log_bin = i.config["log_bin"]
    else:
        log_bin = i.config["log-bin"]

    datadir = Path(data_dir)
    empty = []
    for dir in datadir.iterdir():
        if dir.name.startswith("{}".format(log_bin)):
            empty.append(dir)
    return datadir, log_bin, empty


# Python中执行shell命令，得到返回结果
def exec_bash_cmd(cmd):
    rc, stdout, stderr = exec_cmd(cmd)
    if rc != 0:
        sender.send_all("exec_bash_cmd is failed.cmd:{}.stder:{}".format(cmd, stderr))
    result = stdout.strip()
    return result


# 执行rsync，远程归档传输
def rsync_binlog(binlog, ports):
    dir = archive_today_dir / "{}".format(ports)
    cmd1 = "rsync -avzP --bwlimit=5000 {} {}:{}".format(binlog, NFSHOST, dir)
    rc, stdout, stderr = exec_cmd(cmd1)
    if rc != 0:
        sender.send_all("rsync binlog is failed.cmd:{}.stder:{}".format(cmd1, stderr))


def bak_bin():
    # 获取归档机器中，最大日期的备份目录
    cmd1 = "ssh {}".format(NFSHOST) + ' "/bin/ls {}"'.format(archive_base_dir)
    list_date = exec_bash_cmd(cmd1).split("\n")
    max_date = max(list_date)

    # 在归档机器上创建今天的备份目录
    for port in running_ports():
        cmd_mkdir_date = "ssh {}".format(NFSHOST) + ' "/bin/mkdir -p {}/{}" '.format(archive_today_dir, port)
        rc, stdout, stderr = exec_cmd(cmd_mkdir_date)
        if rc != 0:
            sender.send_all("mkdir archive date dir is failed.cmd:{}.stder:{}".format(cmd_mkdir_date, stderr))

    # 判断上一次的备份端口数和金融端口实例数是否一致（为了防止新添加金融实例，新实例需要备份所有binlog）
    archive_max_dir = archive_base_dir / "{}".format(max_date)
    cmd1 = "ssh {}".format(NFSHOST) + ' "/bin/ls {}"'.format(archive_max_dir)
    list_port = exec_bash_cmd(cmd1).split("\n")

    if len(list_port) != len(financial_ports):
        for p in running_ports():
            # 获得binlog文件的前缀、datadir、以及binlog.index
            log_bin = dir_bin_all_binlog(p)[1]
            datadir = dir_bin_all_binlog(p)[0]
            index = log_bin + ".index"
            binlog_index = datadir / "{}".format(index)

            # 如果不存在于list_port中，证明上次备份没有备份该文件，需要获取所有的binlog
            if str(p) not in list_port:
                all_binlog = dir_bin_all_binlog(p)[2]

                cmd = "/usr/local/mysql/bin/mysql -uroot -S /tmp/mysql_{}.sock -e 'flush logs;'".format(p)
                rc, stdout, stderr = exec_cmd(cmd)
                if rc != 0:
                    sender.send_all("flush log is failed.cmd:{}.stder:{}".format(cmd, stderr))

                # 上一步执行刷新日志命令，因此最新的日志不需要拷贝，需要增加判断
                tail_bin = tail_n(binlog_index, 1).lstrip("./")
                for binlog in all_binlog:
                    if str(binlog) != str(datadir / "{}".format(tail_bin)):
                        rsync_binlog(binlog, p)
            else:
                # 如果存在于list_port中，证明没有新添加金融端口的数据库，拷贝最后一次的binlog文件即可
                tail_bin = tail_n(binlog_index, 1).lstrip("./")
                cmd = "/usr/local/mysql/bin/mysql -uroot -S /tmp/mysql_{}.sock -e 'flush logs;'".format(p)
                rc, stdout, stderr = exec_cmd(cmd)
                if rc != 0:
                    sender.send_all("flush log is failed.cmd:{}.stder:{}".format(cmd, stderr))

                tail_binlog = datadir / "{}".format(tail_bin)
                rsync_binlog(tail_binlog, p)
                rsync_binlog(binlog_index, p)


if __name__ == '__main__':
    bak_bin()

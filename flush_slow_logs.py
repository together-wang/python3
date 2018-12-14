import datetime
from mtk.utils.cmd import exec_cmd
from mtk.scripts.mysql_backup_binlog import dir_bin_all_binlog

# 如需其他实例切割slow.log，在ports增加端口即可
ports = [5004]
today = datetime.date.today()


def week_slow_log():
    for port in ports:
        # dir即获得datadir
        dir = dir_bin_all_binlog(port)[0]

        cmd = "mv {}/slow.log {}/slow-{}.log".format(dir, dir, today)
        rc, stdout, stderr = exec_cmd(cmd)
        if rc != 0:
            print("flush_slow_log.py.mv is failed.stderr:{}".format(stderr))

        cmd = "/usr/local/mysql/bin/mysqladmin -uroot -S /tmp/mysql_{}.sock flush-logs slow".format(port)
        rc, stdout, stderr = exec_cmd(cmd)
        if rc != 0:
            print("flush_slow_log.py.flush-logs slow failed.stderr:{}".format(stderr))


if __name__ == '__main__':
    week_slow_log()

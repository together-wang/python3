#该脚本实现的功能是，对备份机器上的备份进行随机抽取并恢复
import datetime
import random
import sys
from pathlib import Path
import shutil
from mtk.mysql.backup import get_range, DATE_FORMAT, NxinXtraBackupDir
from mtk.utils.cmd import exec_cmd

MYSQL_ARCHIVE_HOME = '/arch/mysql/'
RECOVERY_PATH = '/arch/recovery/base_recovery'


def choice(path):
    return random.choice(list(path.iterdir()))


class Recovery(NxinXtraBackupDir):
    backup_dir_base = Path(MYSQL_ARCHIVE_HOME) / 'xtrabackup'
    recovery_path = Path(RECOVERY_PATH)

    def __init__(self, date: str):
        self.date = datetime.datetime.strptime(date, DATE_FORMAT).date()
        self.last_sunday, self.this_saturday = get_range(self.date)
        self.backup_dir = self.backup_dir_base / "{}_{}".format(self.last_sunday, self.this_saturday)
        self.choose = choice(self.backup_dir)

    def copy_base(self):
        if not self.recovery_path.exists():
            self.recovery_path.mkdir(parents=True)
        else:
            recovery_base = self.recovery_path / 'base'
            if recovery_base.exists():
                shutil.rmtree(recovery_base)

        shutil.copytree(self.choose / 'base', self.recovery_path / 'base')

    def recovery(self):
        random_dir = choice(self.choose)
        if random_dir.name == 'base':
            print("database is ready to recovery full backup,dir = {}".format(random_dir))
            full_recovery(self.recovery_path / 'base')
        else:
            print("database is ready to recovery incremental backup,dir = {}".format(random_dir))
            incr_recovery(random_dir, self.recovery_path / 'base')


def full_recovery(recovery_base):
    cmd = "/usr/bin/xtrabackup  --prepare  --target-dir={}".format(recovery_base)
    print(cmd)
    rc, stdout, stderr = exec_cmd(cmd)
    if rc == 0:
        print('\033[1;31m***recover is done!***\033[0m')
    else:
        print(stderr)


def incr_recovery(last_incr_dir, recovery_base):
    """
    :param last_incr_dir: 日期最大的增备目录
    :param recovery_base: base 目录
    """
    incr_dirs_except_choose = []
    for dir in last_incr_dir.parent.iterdir():
        if dir.name != 'base' and dir.name < last_incr_dir.name:
            incr_dirs_except_choose.append(dir.name)
    incr_dirs_except_choose = sorted(incr_dirs_except_choose)

    # prepare base
    cmd_base = "/usr/bin/xtrabackup --prepare " \
               "--apply-log-only --target-dir={}".format(recovery_base)
    print(cmd_base)
    rc, stdout, stderr = exec_cmd(cmd_base)
    if rc != 0:
        print('\033[1;31m***full recovery is error!*** {} \033[0m'.format(cmd_base))
        print(stderr)
        return
    print('\033[1;31m***full backup recovery is done!***\033[0m')

    # prepare incr_dirs except choose dir
    for dir in incr_dirs_except_choose:
        cmd = cmd_base + " --incremental-dir={}".format(last_incr_dir.parent / dir)
        print(cmd)
        rc, stdout, stderr = exec_cmd(cmd)
        if rc != 0:
            print('\033[1;31m***incremental recovery is error!*** {} \033[0m'.format(cmd))
            print(stderr)
            return
        print('\033[1;31m***incremental backup recovery is done!***\033[0m')

    # prepare choose dir,  do not use --apply-log-only
    cmd = "/usr/bin/xtrabackup --prepare " \
          + " --target-dir={}".format(recovery_base) + " --incremental-dir={}".format(last_incr_dir)
    print(cmd)
    rc, stdout, stderr = exec_cmd(cmd)
    if rc != 0:
        print('\033[1;31m***incremental recovery is error!*** {} \033[0m'.format(cmd))
        print(stderr)
        return
    print('\033[1;31m***full backup recovery is done!***\033[0m')


r = Recovery(sys.argv[1])
r.copy_base()
r.recovery()

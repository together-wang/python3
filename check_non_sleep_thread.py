from mtk.alert import sender
from mtk.mysql.runtime import get_mysql_runtime_instances
from mtk.mysql import Instance


def check_threads(ins: Instance, threshold: int):
    sql = 'select count(*) from information_schema.processlist ' \
          'where command != "sleep" and command !="Connect" and time > 60;'
    count = ins.query(sql)[0][0]

    if count > threshold:
        sender.send_all("check_non_sleep_thread.py, port {} non_sleep threads {} more than {} ."
                        .format(ins.port, count, threshold))


if __name__ == '__main__':
    for port, instance in get_mysql_runtime_instances().items():
        check_threads(instance, 20)

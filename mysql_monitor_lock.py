from mtk.mysql import Instance
from mtk.alert import sender
from mtk.mysql.runtime import get_mysql_runtime_instances


def mdl_lock(ins: Instance):
    # 查询存在lock的线程id以及SQL语句
    sql = "select c.THREAD_ID,SQL_TEXT,b.PROCESSLIST_STATE " \
          "from performance_schema.events_statements_current c " \
          "join (select t.THREAD_ID,t.PROCESSLIST_STATE from information_schema.processlist p  " \
          "join performance_schema.threads t " \
          "on p.id = t.PROCESSLIST_ID where  state like 'Waiting for % lock') b " \
          "on  c.THREAD_ID=b.THREAD_ID"
    result = ins.query(sql)

    # 查询结果为空，无锁则返回，不继续执行命令
    if len(result) == 0:
        return

    # 循环返回的结果，按照指定的格式打印
    empty = []
    for i in result:
        t_id, sql, lock = i
        message = "thread_id:{}. sql:{}. lock:{} .".format(t_id, sql, lock)
        empty.append(message)
    empty = ','.join(empty)
    message = "mysql_monitor_lock.py.port:{},{}".format(ins.port, empty)
    sender.send_all(message)


if __name__ == '__main__':
    for port, instance in get_mysql_runtime_instances().items():
        mdl_lock(instance)

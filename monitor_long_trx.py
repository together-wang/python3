from mtk.mysql import Instance
import pickle
from mtk.alert import sender
from pathlib import Path
from mtk.mysql.runtime import get_mysql_runtime_instances

interval = 600


# 该函数用来判断数据库中是否有长事务，如果存在长事务，存入字典中
def long_trx(ins: Instance):
    trx_query = '''
        SELECT trx_id,trx_started,timestampdiff(SECOND,trx_started,current_timestamp)
        AS trx_time FROM information_schema.innodb_trx 
        WHERE trx_started<= CURRENT_TIME - {};'''.format(interval)

    trx_sql = ins.query(trx_query)
    # 判断数据库中是否存在长事务，不存在则返回
    if len(trx_sql) == 0:
        return

    list_thread = []
    for i in trx_sql:
        thread = i[0]
        list_thread.append(thread)

    # 得到长事务的事务id，事务开始时间、以及SQL语句，写入字典中
    current_sql = "SELECT any_value(trx.trx_id),group_concat(trx.trx_started,',',cur.sql_TEXT) " \
                  "FROM information_schema.innodb_trx trx " \
                  "JOIN  performance_schema.threads th " \
                  "ON trx.trx_mysql_thread_id=th.PROCESSLIST_ID " \
                  "JOIN performance_schema.events_statements_current cur " \
                  "ON th.THREAD_ID=cur.THREAD_ID " \
                  "WHERE sql_TEXT is not null group by trx.trx_id;"
    mess_dict = {}
    for j in ins.query(current_sql):
        if j[0] in list_thread:
            mess_dict["{}".format(j[0])] = "{}".format(j[1])

    return mess_dict


if __name__ == '__main__':
    for port, instance in get_mysql_runtime_instances().items():
        # dict变量返回是字典，如果为None，说明数据库中不存在长事务
        dict = long_trx(instance)
        if dict is None:
            continue
        else:
            # 如果存在长事务，需要将其序列化到列表，下次执行脚本时，判断dict中的key值是否有存在于序列化列表中，存在则报警
            dir_file = Path("/root/dict_long_file")
            if not dir_file.exists():
                dictfile = open("/root/dict_long_file", 'wb')
                pickle.dump(dict, dictfile)
                dictfile.close()

            dictfile = open("/root/dict_long_file", 'rb')
            readdict = pickle.load(dictfile)

            for k in dict.keys():
                if k in readdict:
                    sql = readdict["{}".format(k)]
                    sender.send_all(
                        "mysql_long_trx_monitor.py.Monitor for long transactions,port:{}. trx_id:{}. sql:{}.".format(
                            port, k, sql))
            dictfile = open("/root/dict_long_file", 'wb')
            pickle.dump(dict, dictfile)
            dictfile.close()

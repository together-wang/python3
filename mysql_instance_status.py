#检查mysql实例的状态
from mtk.mysql.runtime import get_mysql_running_ports, get_mysql_config_files #得到MySQL实例running状态的端口，以及配置文件的端口
from mtk.alert import sender #发送邮件报警
import datetime

running_ports = get_mysql_running_ports()
config_ports = get_mysql_config_files()
#判断mysql实例running的端口与配置文件中获取的端口是否一致，不一致，证明有宕机现象
if sorted(running_ports) != sorted(config_ports):
    for port in config_ports:
        if port not in running_ports:
            datetime_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sender.send_all("{}:mysql port {} is not running".format(datetime_now, port))

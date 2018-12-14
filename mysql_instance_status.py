from mtk.mysql.runtime import get_mysql_running_ports, get_mysql_config_files
from mtk.alert import sender


def check(c_ports, r_ports):
    not_running_port = []
    for port in c_ports:
        if port not in r_ports:
            not_running_port.append(port)
    return not_running_port


running_ports = get_mysql_running_ports()
config_ports = get_mysql_config_files()
if sorted(running_ports) != sorted(config_ports):
    not_running_ports = check(config_ports, running_ports)
    sender.send_all("mysql_instance_status_check.py.mysql port {} is not running".format(not_running_ports))


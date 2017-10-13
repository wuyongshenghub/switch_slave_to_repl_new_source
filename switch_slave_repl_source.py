#encoding:utf-8
# 将从库同步数据源1切换到数据源2
import sys
import commands
import json
import time

user = 'root'
upwd = '123456'
flag = "mysql: [Warning] Using a password on the command line interface can be insecure."

def str_to_json(arr):
    # 字符串转换成字典
    dtmp = {}
    for i in range(1,len(arr),2):
        dtmp[arr[i-1].replace(':','')] = arr[i]
    return dtmp

def show_z_master_status(ip,port,user,pwd):
    #打印z库当前使用的二进制name及pos
    z_cmd_str = "show master status \G"
    z_cmd = "/path/bin/mysql -h %s -P %d -u%s -p'%s' -e '%s'|egrep -i 'File|Position'"%(ip,port,user,pwd,z_cmd_str)
    status_res = commands.getstatusoutput(z_cmd)
    if status_res[0] == 0:
        ztmp =  status_res[1].replace(flag,'').strip('\n').split()
        return str_to_json(ztmp)
    else:
        print "\033[31;1m%s:%s打印失败\033[0m"%(ip,port)
        sys.exit(1)

def change_newip_slave(ip,port,user,pwd,z_logfile_name,z_logfile_pos,n_ip,n_port,o_ip_m,o_port_m):
    #变更新ip同步的数据源
    # stop slave;reset slave all;
    n_cmd_str = "stop slave;select sleep(2);reset slave all"
    n_change_master = 'CHANGE MASTER TO MASTER_HOST="%s",MASTER_USER="test_repl",MASTER_PASSWORD="123456",MASTER_PORT=%d,' \
                      'MASTER_LOG_FILE="%s",MASTER_LOG_POS=%d,MASTER_CONNECT_RETRY=10 for channel "m%s_%d";'\
                      %(ip,int(port),z_logfile_name,int(z_logfile_pos),int(ip.split('.')[3]),int(port))
    n_start_slave = 'start slave for channel "m%s_%d"'%(int(ip.split('.')[3]),int(port))
    n_cmd = "/path/bin/mysql -h %s -P %d -u%s -p'%s' -e '%s;select sleep(2);%s select sleep(2); %s;'"\
            %(n_ip,int(n_port),user,pwd,n_cmd_str,n_change_master,n_start_slave)
    print n_cmd
    change_master_res = commands.getstatusoutput(n_cmd)
    if change_master_res[0] == 0:
        print "\033[32m%s:%s success\033[0m"%(n_ip,n_port)
        # 释放全局读锁
        unlock = "unlock tables"
        un_cmd = "/path/bin/mysql -h %s -P %d -u%s -p'%s' -e '%s'"%(o_ip_m,int(o_port_m),user,pwd,unlock)
        print un_cmd
        un_res = commands.getstatusoutput(un_cmd)
        if un_res[0] == 0:
            print "\033[32m%s:%s释放锁完成\033[0m"%(o_ip_m,o_port_m)
        
    else:
        print "\033[31;1mfail:%s:%s\033[0m"%(n_ip,n_port)
        sys.exit(1)

# main入口
with open('iplist') as f:
    for line in f:
        if not line.startswith('#'):
            line = line.strip().split()
            o_ip_m =  line[0]
            o_port_m = line[1]
            o_ip_s = line[2]
            o_port_s = line[3]
            z_ip_m = line[4]
            z_port_m = line[5]
            n_ip = line[6]
            n_port = line[7] # 可定义port
            #print o_ip_m,o_port_m,o_ip_s,o_port_s,z_ip_m,z_port_m,n_ip,n_port
            # 加全局读锁 flush tables with read lock
            str = "flush tables with read lock;"
            o_cmd = "/path/bin/mysql -h %s -P %d -u %s -p'%s' -e '%s select sleep(10);'"%(o_ip_m,int(o_port_m),user,upwd,str)
            result = commands.getstatusoutput(o_cmd)
            if result[0] != 0:
                print "\033[31;1mip:%s-port:%s,\n加全局读锁失败:%s\033[0m"%(o_ip_m,o_port_m,result[1].replace(flag,'').strip())
                sys.exit(1)
            else:
                # 加锁成功后 比较o库与z库同步的master_log_file/master_log_pos
                print "\033[32m等待10秒...\033[0m"
                time.sleep(10)
                print "\033[32m开始执行...\033[0m"
                o_str_s = "show slave status \G"
                o_cmd_s = "/path/bin/mysql -h %s -P %d -u %s -p'%s' -e '%s;'|egrep -i 'Master_Host|Master_Port|Master_Log_File|Read_Master_Log_Pos'|grep -v 'Relay_Master_Log_File'"%(o_ip_s,int(o_port_s),user,upwd,o_str_s)
                oresult = commands.getstatusoutput(o_cmd_s)
                #print result[1].replace(flag,'')
                z_cmd_m = "/path/bin/mysql -h %s -P %d -u %s -p'%s' -e '%s;'|egrep -i 'Master_Host|Master_Port|Master_Log_File|Read_Master_Log_Pos'|grep -v 'Relay_Master_Log_File'"%(z_ip_m,int(z_port_m),user,upwd,o_str_s)
                zresult = commands.getstatusoutput(z_cmd_m)
                #print zresult
                # 比较o库与z库 执行show slave status 状态值是否相等
                if (oresult[0] == 0 and zresult[0] == 0):
                    print "\033[32mo从库%s:%s状态%s\033[0m"%(o_ip_s,o_port_s,oresult[1].replace(flag,''))
                    print '*'*20
                    print "\033[32mz从库%s:%s状态%s\033[0m"%(z_ip_m,z_port_m,zresult[1].replace(flag,''))
                    otmp = oresult[1].replace(flag,'').strip('\n').split()
                    ztmp = zresult[1].replace(flag,'').strip('\n').split()
                    # 字符串转换dict
                    odict = str_to_json(otmp)
                    zdict = str_to_json(ztmp)
                    # o库二进制名/pos
                    omlf = odict['Master_Log_File']
                    ormlp = odict['Read_Master_Log_Pos']
                    # z库二进制名/pos
                    zmlf = zdict['Master_Log_File']
                    zrmlp = zdict['Read_Master_Log_Pos']
                    if (omlf == zmlf and ormlp == zrmlp):
                        # 进行ip同步数据源1切换同步数据源2
                        # 获取z库当前二进制name/pos
                        z_status_res = show_z_master_status(z_ip_m,int(z_port_m),user,upwd)
                        zmaster_log_file = z_status_res['File']
                        zmaster_log_file_pos = z_status_res['Position']
                        # 变更n_ip同步的数据源
                        change_newip_slave(z_ip_m,z_port_m,user,upwd,zmaster_log_file,zmaster_log_file_pos,n_ip,
                                           n_port,o_ip_m,o_port_m)
                else:
                    print "\033[31;1m %s:%s-%s:%s失败,检查原因\033[0"%(o_ip_s,o_port_s,z_ip_m,z_port_m)
                    sys.exit(1)

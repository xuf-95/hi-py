import sys, re
from datetiem import datetime, date, time
from odps.models import Column, Partition, Schema

# set encoding
reload(sys)

# get task run time
today = datetime.now.strftime('%Y-%m-%d %H:%M:%S')


# get project name
pro_name = o.get_project().name


# get task instance
for ins in o.list_instance(only_owner=False,status='Terminated'):
    
# 遍历示例方法的属性

# 数据处理将数据封装到对应的表中

from datetime import datetime, date
from odps import ODPS
from odps.models import Schema, Column, Partition
from odps.df import DataFrame
from odps.df import output
import pandas as pd
from odps import options
import time
import os, sys, re, json

reload (sys)
#修改系统默认编码。数据中存在中文字符时需要执行此操作。
sys.setdefaultencoding('utf8')

def get_odps_columns_json (columns):
    """
    定义函数解析Columns，转换成Json数组
    """
    column_list = []
    for i in range (len(columns)):
        column = columns[i]
        name, type, comment = column.name, column.type, column.comment
        column_dict = {"name":name, "type":type.name, "comment":comment}
        column_list.append(column_dict)
    col_pd = pd.DataFrame(column_list)
    return col_pd.to_json(orient="records", force_ascii=False)

def exe_sql(sql):
    """
    通过SQL获取表的记录数
    """
    re_cnt = 0 
    with o.execute_sql(sql).open_reader() as reader:
        for record in reader[:]:
            re_cnt = record[0]
    return re_cnt

def get_odps_partitions_json(tbl):
    """
    计算表的数据量。对于分区表计算分区时间对应的数据量，并按分区时间倒叙排序。全量表直接计算数据量
    """
    # 
    pt_list = []
    pt_dict = []
    data_dict={}
    pt_json_list = {}
    if tbl.schema.partitions: # 判断是否是分区表
        with o.execute_sql("select {1} as pt_name,count(*) from {0} group by {1}".format(tbl.name, tbl.schema.partitions[0].name)).open_reader() as reader:
            list_key = []
            list_value = []
            for record in reader[:]:
                list_key.append(str(record[0]))
                list_value.append(int(record[1]))
                lt_dt_zip = zip(list_key, list_value)
                lt_dt = dict(lt_dt_zip) 
            lt_dt_zip = zip(list_key, list_value)
            lt_dt = dict(lt_dt_zip)
        try :
            pt_list_asc = list(zip(lt_dt.values(), lt_dt.keys()))
            pt_list = sorted(pt_list_asc, reverse = True)
        except Exception as e:
            print (e)

        for i in range(len(pt_list)):
            pt_info = {"pt_name":pt_list[i][1], "pt_cnt":pt_list[i][0]}
            pt_dict.append(pt_info)
        
        for i in range(len(pt_dict)):
            pt_dict2 = {"" + str(i + 1): pt_dict[i]}
            pt_json_list.update(pt_dict2)
        try:
            data_dict = {"tbl_attr" : 0, "pt_name" : tbl.schema.partitions[0].name ,"data" : pt_json_list} # 0 代表分区表
            str_data_dict = str(data_dict).replace("'",'"')            
        except  Exception as e:
            print (e)

    else:
        with o.execute_sql("select count(*) from {0}".format (tbl.name)).open_reader() as reader:
            for record in reader:
                pt_dict_full_info = {"pt_name" : "null", "pt_cnt" : int(record[0])} # 全量表对应的条数
                pt_json_list.update(pt_dict_full_info)
        data_dict ={"tbl_attr" : 1, "pt_name" : "null", "data" : pt_json_list} # 1 代表全量表
        str_data_dict = str(data_dict).replace("'",'"')
    return str_data_dict
# 执行时间 
start_tm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# 定义表结构结构 
table_name = 'dim_meta_table_info'
table_name_err = 'dim_meta_table_error'
columns = [Column(name='tbl_name', type='string', comment='表名'),
           Column(name='tbl_comment', type='string', comment='表注释'),
           Column(name='tbl_owner', type='string', comment='作者'),
           Column(name='tbl_level', type='string', comment='层级'),
           Column(name='index1_name', type='string', comment='一级标签'),
           Column(name='index2_name', type='string', comment='二级标签'),
           Column(name='index3_name', type='string', comment='三级标签'),
           Column(name='tbl_project', type='string', comment='表所属项目空间'),
           Column(name='tbl_col_num', type='int', comment='字段个数'),
           Column(name='tbl_col_json', type='string', comment='字段Json'),
           Column(name='tbl_pt_name', type='string', comment='（如果是分区表）分区名'),
           Column(name='tbl_pt_num', type='string', comment='分区个数'),
           Column(name='tbl_total_cnt', type='string', comment='记录数'),
           Column(name='tbl_pt_cnt', type='string', comment='最新分区记录数'),
           Column(name='tbl_create_tm', type='string', comment='创建时间'),
           Column(name='tbl_ddl_tm', type='string', comment='最近创建时间'),
           Column(name='tbl_mod_tm', type='string', comment='最近更新时间'),
           Column(name='tbl_lifecycle', type='int', comment='数据生命周期'),
           Column(name='tbl_type', type='string', comment='是否是内部表'),
           Column(name='tbl_size', type='string', comment='占用空间，字节'),
           Column(name='run_tm', type='string', comment='运行时间')]

columns_err = [Column(name='tbl_name', type='string', comment='表名'),
           Column(name='run_tm', type='string', comment='运行时间')]

partitions = [Partition(name='dt', type='string', comment='按日期yyyymmdd分区')]
schema = Schema(columns=columns, partitions=partitions)
schema_err = Schema(columns=columns_err, partitions=partitions)

# 存放解析的数据
records = []
# 存放异常数据
records_err = []
# 成功的记录条数
sn = 0
# 失败的记录条数
en = 0

tbl_name = ""
tm = datetime.now()

# 存放环境中一共有多少张表
total_tbl_cnt = []
for tbl in o.list_tables():
    total_tbl_cnt.append(tbl.name)
total_tbl_len = len(total_tbl_cnt)
print("改项目空间一共有{}表".format(total_tbl_len))

# tbl = o.get_table('ods_mz_warn_core')

for tbl in o.list_tables():
    try :
        task_run_start_tm = time.time()
        tbl_schema = tbl.schema
        tbl_columns = tbl_schema.columns
        tbl_col_json = get_odps_columns_json(tbl_columns)
        tbl_name = tbl.name
        tbl_comment = tbl.comment
        tbl_level, index1_name, index2_name, index3_name = None, None, None, None 
        if re.match(r'ods|dwd|dim|dws|dwt|ads|adi',tbl_name.lower()): 
            tbl_level = tbl_name.lower()[:3].upper()
        col_arr = tbl_name.split('_')
        # 一级标签
        if(len(col_arr)>1) :
            index1_name = col_arr[1].lower()
        # 二级标签
        if(len(col_arr)>2) :
            index2_name = col_arr[2].lower()
        # 三级标签
        if(len(col_arr)>3) :
            index3_name = col_arr[3].lower()
        tbl_owner = tbl.owner.split(':')[-1]
        tbl_project = tbl.project.name
        tbl_col_num = len(tbl_columns) 

        tbl_pt_name = get_odps_partitions_json(tbl)
        ret_json = tbl_pt_name
        tbl_pt_num = None 
        tbl_total_cnt_sql = "SELECT COUNT(*) FROM {};".format (tbl_name)
        tbl_total_cnt, tbl_pt_cnt = 0, 0
        tbl_total_cnt = exe_sql(tbl_total_cnt_sql)
        tbl_create_tm = tbl.creation_time.strftime('%Y-%m-%d %H:%M:%S')
        tbl_ddl_tm = tbl.last_meta_modified_time.strftime('%Y-%m-%d %H:%M:%S')
        tbl_mod_tm = tbl.last_modified_time.strftime('%Y-%m-%d %H:%M:%S')
        tbl_lifecycle = tbl.lifecycle
        tbl_type = tbl.is_virtual_view
        tbl_size = tbl.size
        run_tm = tm.strftime('%Y-%m-%d %H:%M:%S')
        task_run_end_tm = time.time()

        # 任务运行时间
        run_time = task_run_end_tm - task_run_start_tm 
        # print ("table name : {} tbl_total_cnt : {} task_progress : {}/{} run_time : {} ret_json : {}".format (tbl_name,tbl_total_cnt, en + sn + 1, total_tbl_len, run_time, ret_json))  # tbl_total_cnt, tbl_pt_cnt
        print ("{}&{}".format (tbl_name, ret_json))  # tbl_total_cnt, tbl_pt_cnt
        records.append ([tbl_name, tbl_comment, tbl_owner, tbl_level, index1_name, index2_name, index3_name, tbl_project, 
            tbl_col_num, tbl_col_json, tbl_pt_name, tbl_pt_num, tbl_total_cnt, tbl_pt_cnt, tbl_create_tm, tbl_ddl_tm, tbl_mod_tm, tbl_lifecycle, tbl_type, tbl_size, run_tm])
    except Exception as e:
        en = en + 1
        records_err.append([tbl_name,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        print(tbl_name, e)
    else :
        sn = sn + 1

# 定义分区表的分区时间为当天时间(yyyymmdd)            
partition = '%s=%s' % (partitions[0].name, datetime.now().strftime('%Y%m%d'))

# 不存在则创建表
to_tbl = o.create_table(table_name, schema, if_not_exists = True, lifecycle = 30)

# 起到覆盖分区的作用
to_tbl.delete_partition(partition, if_exists = True)

# 数据写入到数据表中
o.write_table(table_name, records, partition=partition, create_partition=True)

# 结束时间 
end_tm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print ("总计%d条，成功写入%d条，写入失败%d条    开始时间:%s     结束时间：%s " %((sn+en), sn, en, start_tm, end_tm))


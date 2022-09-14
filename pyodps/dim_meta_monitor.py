from datetime import datetime, time 
from odps import ODPS
from odps.models import Schema, Column, Partition
from odps.df import DataFrame
from odps.df import output
from odps import options

# def tb name
dim_tb_name = "dim_meta_tb_change_sub"
dim_tb_col_name = "dim_meta_tb_col_sub"

# def dim_tb_name col
tb_columns = [
    Column(name = 'tb_schema', type = 'string', comment = '库名称')
    ,Column(name = 'tb_name', type = 'string', comment = '表名称')
    ,Column(name = 'tb_status', type = 'string', comment = '表状态')
    ,Column(name = 'run_time', type = 'string', comment = '运行时间')
]

# def dim_tb_col_name col
col_columns = [
    Column(name = 'tbl_schema', type = 'string', comment = '库名称'),
    Column(name = 'tbl_name', type = 'string', comment = '表名称'),
    Column(name = 'col_name', type = 'string', comment = '字段名称'),
    Column(name = 'col_status', type = 'string', comment = '字段状态'),
    Column(name = 'col_detail', type = 'string', comment = '字段变更详情'),
    Column(name = 'run_time', type = 'string', comment = '运行时间')
    ]

# def partition
partitions = [Partition(name = 'dt', type = 'string', comment = '按日期yyyymmdd分区')]
dim_tb_schema = Schema(columns = tbl_columns, partitions = partitions)
dim_tb_col_schema = Schema(columns = tbl_columns, partitions = partitions)

# key: 
get_ld_tb = o.get_table('')
get_ld_tb_ = o.get_table('')

# def table executer function
def tb_exe_sql(sql):
    result_list = []
    with o.execute_sql(sql).open_reader() AS reader:
        for record in reader(:):
            for i in range(len(record)):
                result_list.append(record[i])
    return result_list

# def column executer function
def tb_col_exe_sql(sql):
    result_col_list = []
    with o.execute_sql(sql).open_reader() AS reader:
        for rocord in reader[:]:           
        result_col_list.append([record[0],record[1],record[2],record[3],record[4],record[5],record[6],record[7],record[8],record[9]])
    return result_col_list

# show today and yesterday
td = datetime.datetime.today()
td_time = td.date() - datetime.timedelta(days=1)
yes_time = td.date() - datetime.timedelta(days=2)
td_date = td_time.strftime('%Y%m%d')
yes_date = yes_time.strftime('%Y%m%d')


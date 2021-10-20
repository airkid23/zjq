"""
                    
                    功能：
                        检查定岗过程映射表
                    注意：
                        注释含有 '★' 的代表需要手动修改相关参数 
print('■'*20)
"""
import pandas as pd
import pymysql
import os
from impala.dbapi import connect # 链接数仓依赖包

# impala连接配置
impalaConn = connect(host='172.16.10.33', port=21050, database="`data`")
impalaCursor = impalaConn.cursor()

# 小而美正式服配置
class conn_database:
    def __init__(self):
        self.conn = pymysql.connect(
          host = "120.24.151.26" ,
               port = 3306,
               user = 'root',
               password = "Zjq_20190606",
               db = 'bigdata_final',# bigdata_test  statistics_202004
               charset = 'utf8'
                       )
    def get_cursor(self):
        return self.conn.cursor()
conn = conn_database().conn 
cursor = conn.cursor(cursor=pymysql.cursors.DictCursor)


# 数仓dt_dg
#sql = """
#select distinct fs_dt_level4 FROM major.fs_dg
#"""
#impalaCursor.execute(sql)
#dg_total = pd.DataFrame(list(impalaCursor.fetchall())).rename(columns={0:"fs_dt_level4"}) 
#
#dg_temp_df = dg_total[dg_total['fs_dt_level4'] == 'ae特效师']


# 数仓数据组数据
sql = """
select distinct fs_dt_level4 FROM `data`.dw_std_year
"""
impalaCursor.execute(sql)
dt_total = pd.DataFrame(list(impalaCursor.fetchall())).rename(columns={0:"dt_4"}) 

#%%表结构修改，剔除源代码中的小而美表的四级(★★★★★★★已修改★★★★★★★)
######################

# 数仓小而美数据
sql = """
select distinct fs_xem_level1,fs_xem_level2,fs_xem_level3 FROM `data`.dw_xem_year
"""
impalaCursor.execute(sql)
xem_total = pd.DataFrame(list(impalaCursor.fetchall())).rename(columns={0:"xem_1",1:"xem_2",2:"xem_3"})


#%% 文件读取

# 定岗过程映射表--文件路径 ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
FilePath = r'Y:\3、产业集群分析\新能源汽车产业链\Result'
# 定岗过程映射表--名称 
Filename = '产业链定岗映射过程表.xlsx' 


# 读取定岗过程表结果集,并且修改列名称
result_df = pd.read_excel(os.path.join(FilePath,Filename)).rename(columns={"序号":'id',
    '定岗名称':'job_name','岗位ID':'job_id', '备注':'remark','关联度':'rate', '关键词':'keyword',
    '岗位描述':'description'})

#%% 职能体系表修改(★★★★★★★待修改★★★★★★★)

# 现行定岗详情  201911-202010
path = r'X:\0.核心资料\1.职能体系\现行版本\小而美-数据组的职能体系映射表(含id、需求量：201911-202010）_V1.1.xlsx'
job_detail = pd.read_excel(path,sheet_name = '定制岗详情表')    
xem_detail = pd.read_excel(path,sheet_name = '标准岗位映射表').drop(index=0,axis=0)
xem_detail.columns = ['xem_1_id','xem_1','xem_2_id','xem_2','xem_3_id',\
                      'xem_3','xem_count','dt_4','dt_count','support']   

#%% 检查
# 定岗总数 与 计算总和不一致则代表漏写remark备注
post_total = result_df['job_name'].value_counts().count()

colulate_remark = result_df['remark'].value_counts().sum()
if  colulate_remark < post_total:
    raise print('■'*20,'计算总数为(定岗总数与计算总数不一致，需要注意是否有忘记remark岗位)：',colulate_remark)
# 是否在过程映射表中重复
if len(set(result_df['job_name'].value_counts().index.tolist())) < len(result_df['job_name'].value_counts().index.tolist()):
    raise print('■'*20,'定岗过程映射表中出现岗位名称重复')

    
#%% 数据集拆分0123

# 定岗映射过程表的列名称    
columns_name = ['id', 'FS_xem_Level1', 'FS_xem_Level2', 'FS_xem_Level3', 'job_name',
       'job_id', '岗位池', 'FS_dT_Level4', 'remark', 'description',
       'IndustryID_Add100', 'IndustryID_Add50', 'Industry_name', '改正']
# 数据集筛选0123处理且重设索引
remark_list = result_df['remark'].value_counts().index.tolist()
if 0 in remark_list:
    remark_0_df = result_df[result_df['remark'] == 0].reset_index(drop=True)
else:
    remark_0_df = []
if 1 in remark_list:
    remark_1_df = result_df[result_df['remark'] == 1].reset_index(drop=True)
else:
    remark_1_df = []
if 2 in remark_list:
    remark_2_df = result_df[result_df['remark'] == 2].reset_index(drop=True)
else:
    remark_2_df = []
if 3 in remark_list:
    remark_3_df = result_df[result_df['remark'] == 3].reset_index(drop=True)
else:
    remark_3_df = []


#%% 检查标准岗数据集  【这里会出现等待，查询小而美本地数据库】
#判断是否存在标准岗
if len(remark_0_df) != 0:
    # 1、FS_xem_level3 与 job_name 名称是否一致
    error_1 = remark_0_df['job_name'][remark_0_df['job_name'] != remark_0_df['FS_xem_Level3']].tolist()
    # 2、过程映射表自己填写的 FS_xem_level1-3 与数仓的 FS_xem_leve1-3一致
    for index,row in remark_0_df.iterrows():# 遍历每一行，且行索引
        # row series对象
        temp_local_data =  remark_0_df[remark_0_df['job_name'] == row['job_name']]
        temp_xem_data = xem_detail[xem_detail['xem_3'] == temp_local_data['job_name'][index]].head(1)# temp_xem_data 
        if len(temp_xem_data) == 0:
            raise print('定岗名称错误,error_1 还没改：',row['job_name'])
        error_2 = temp_local_data['job_name'][\
                                 (temp_local_data['FS_xem_Level1'] != temp_xem_data['xem_1'].values )|\
                                 (temp_local_data['FS_xem_Level2'] != temp_xem_data['xem_2'].values )|\
                                 (temp_local_data['FS_xem_Level3'] != temp_xem_data['xem_3'].values )\
                                 ].tolist()
        
    
    # 3、标准岗 job_id 是否填写
    error_3 = remark_0_df['job_name'][remark_0_df['job_id'].isnull()].tolist()
    # 4、引用标准岗的job_id是否正确，通过查询数据库是否存在此表，且检查此表是否有数据
    jobs_dcit = dict(zip(\
                    remark_0_df['job_name'].values,\
                    remark_0_df['job_id'].values.astype(int).tolist()))
    
    # jobs_dcit = {'有表无数据':3040200313,'无表':3040200314} # 测试集
    error_4_1 = []
    error_4_2 = []
    for job_name,job_id in jobs_dcit.items():
        try:
            cursor.execute('select * from tb_job_{}'.format(job_id))
        except:
            # 没有表，即标准岗位是否用对版本如【201912-202012】
            error_4_1.append({job_name:job_id})
            continue
        if len(list(cursor.fetchall())) == 0 :
            # 有表没有数据，存在操作不当导致数据缺失
            error_4_2.append({job_name:job_id})            
    # 错误规整
    print('■'*20,'定岗名称与小而美三级名称不一致：',error_1)
    print('■'*20,'过程映射表123级，与数仓123级对应不上：',error_2)
    print('■'*20,'标准岗 job_id 没有填写：',error_3)
    print('■'*20,'引用标准岗,在数据库没有表：',error_4_1)
    print('■'*20,'引用标准岗,在数据库有表但没有数据：',error_4_2)
    
# %% 检查数据集 1 (错误想法)           
if len(remark_1_df) != 0:
    
    # 1、ID为空
    error_1 = remark_1_df['job_name'][remark_1_df['job_id'].isnull()].tolist()
     
    # 2、FS_xem_Level3为空
    error_2 = remark_1_df['job_name'][remark_1_df['job_id'].notnull()].tolist()
    
    error_3 = []
    error_4 = []
    error_5 = []
    error_6 = []
    for i in remark_1_df['job_name']:
        #获取定制岗index
        job_index = job_detail[job_detail['定岗名称'] == i]
        remark_1_index = remark_1_df[remark_1_df['job_name'] == i]
        # 3、定岗名称在定制岗中找不到
        if len(job_index.index) == 0:
            error_3.append(i)
        # 4、ID错误
        try:
            if int(remark_1_index['job_id'].values) != int(job_index['岗位ID'].iloc[0]):
                error_4.append(i)
        except:
            error_4.append(i)
            
        # 5、FS_xem_Level2错误
        if str(remark_1_index['FS_xem_Level2'].values) != str(job_index['fs_xem_level2'].values):
            error_5.append(i)
        
        # 6、FS_xem_Level1错误
        if str(remark_1_index['FS_xem_Level1'].values) != str(job_index['fs_xem_level1'].values):
            error_6.append(i)
        
        
#%% 检查数据集2
if len(remark_2_df) != 0:
    
 
    dt_all_jobs_name = dt_total['dt_4'].value_counts().index.tolist() # 数仓数据组所有岗位
    
    # 1、校验 ,映射表1-2，岗位名称，重新映射名称为空的岗位
    error_2_1 = remark_2_df['job_name'][remark_2_df['FS_xem_Level1'].isnull()|\
                remark_2_df['FS_xem_Level2'].isnull()|\
                remark_2_df['job_name'].isnull()|\
                remark_2_df['FS_dT_Level4'].isnull()    
                ]
    if len(error_2_1) != 0:
        raise print('存在错误，映射表FS_xem_level 1-2/岗位名称/重新映射名称为空的岗位：',error_2_1)
    
    # 2、四级表为，小而美平台映射到了，但是没有数据的岗位重新提取
    # 情况1：
    # 定岗名称，数据组四级名称一致，但是数据组没有数据
    # 因此下面代码，有新增数据组四级岗位数据集表格才执
    # 新增四级岗位表数据为链路数仓 std_year_dropdup
    
    try:
        # 新增数据组四级表读取
        level_4_df = pd.read_csv(os.path.join(FilePath,'新增数据组四级岗位数据集.csv'))
        level_4_jobs = level_4_df['fs_dt_level4'].unique().tolist() # 新增数据组四级岗位名称
        flag = True
    except:
        flag = False # 代表没有新增四级数据集，不需要链路数仓，且不需要执行下面校验代码
   

    
    # 3、校验映射是否存在,且检查，业务人员在 “新增数据组四级岗位名称.xls” 是否填充了缺失数据集
        """
        此岗位完全映射，即过程映射表FS_dT_Level4填写的岗位，在小而美数据组表中皆能找到数据集
        出现没有映射岗位的，即找不到，那么是业务人员注意修改此岗位重映射，增或者删除       
        """
    if flag:
        needAddData_jobs = [] # 此变量，存储需要在 “新增数据组四级岗位名称.xls” 表中找到新增数据集

        for index,row in remark_2_df.iterrows():
            # 映射拆分
            mapjobs = row['FS_dT_Level4'].split('|')
            # 实际映射岗位数量
            realNum = 0
            # 过程映射表实际需要映射岗位数量
            jobNum = len(mapjobs)
            # 从 std_year_dropup 数据组查询，查到岗位即存在数据集 ▲▲▲▲▲▲▲▲▲▲▲▲▲▲▲
            for job in mapjobs:
                temp_xem_df = dt_total[dt_total['dt_4'] == job]
                
                if len(temp_xem_df) != 0:
                    realNum+=1
                else:
                    needAddData_jobs.append(job)
                    
        # 判断数仓人员忘记链路，还是业务人员错误        
        for i in needAddData_jobs:
            if i not in level_4_jobs:
                print('业务人员在新增数据组四级.xls 没有提岗位数据:',needAddData_jobs)
            else:
                print('数仓人员没有链路 新增数据组四级岗位名称.xls 上数仓(impala 是否同步)：',i)
                
"""
    这里的针对新增数据组四级里面的岗位，在数仓spark数仓链路处理数据的时候,可能在fs_dg找不到对应的映射关系
    针对性的校验，如果找不到，可以将这个岗位数据集，提出来作为新增3级数据集处理,如果是某个岗位下的映射，可以删除
"""


## 数仓提取fs_dg数据集
sql = """
 select * from major.fs_dg 
"""
impalaCursor.execute(sql) 
temp_df = pd.DataFrame(list(impalaCursor.fetchall())).rename(
        columns={0:'id_1',1:'fs_dt_level1',2:'id_2',3:'fs_dt_level2',
                 4:'id_3',5:'fs_dt_level3',6:'id_4',7:'fs_dt_level4'}
        )

# 读取新增数据组四级映射表
add_dt_df = pd.read_csv(os.path.join(FilePath,'新增数据组四级岗位数据集.csv'))
jobs_name_list = add_dt_df['fs_dt_level4'].value_counts().index.tolist()

# 去查询是否有
for job in jobs_name_list:
    df = temp_df[temp_df['fs_dt_level4'] == job ]
    if len(df) == 0:
        print('数仓fs_dg无法识别转换此岗位的映射，如果是一个岗位对应一个映射，将其改为3，并将新增数据四级\
              里面这个岗位数据集，提出来放到新增小而美三级表里面即可，如果是一个个岗位下多个映射其中一个，可以考虑将其删除',job)
#    if df['id_1'].values[0] == 'null':
#        print('数仓fs_dg无法识别转换的映射岗位，如果是单独岗位，将其转换为3，如果是某个岗位下的映射，将其删除',job)
        
                        
              
#%% 检查数据集 3            
# 读取新增三级数据集
if len(remark_3_df) != 0:                                                          
    MAJOR_PATH = os.path.join(FilePath, '{}.csv'.format('新增小而美三级岗位数据集'))            
    with open(MAJOR_PATH,'r',encoding='utf-8') as f:
        level_3_df = pd.read_csv(f).rename(columns={"parentID":"code","fs_xem_level3":"name","fs_dt_level4":"name","demand":"count","idustry_level2":"business"})
    
    level_3_jobs = level_3_df['name'].value_counts().index.tolist()

    remark_3_jobs = remark_3_df['job_name'].value_counts().index.tolist()            
    
    # 1、校验过程映射表 与 新增小而美三级岗位数据集，差集  【如果两个语句打印为空，跳过】
    
    # 新增小而美三级岗位数据集有，但是定岗过程映射表没有的岗位
    error_3_1 = list(set(level_3_jobs).difference(set(remark_3_jobs)))
    # 定岗过程映射表中有，但是新增小而美三级岗位数据集没有的岗位
    error_3_2 = list(set(remark_3_jobs).difference(set(level_3_jobs)))
    if len(error_3_1) or len(error_3_2) != 0:  
        print('新增小而美三级岗位数据集有，但是定岗过程映射表没有的岗位：\n',error_3_1)
        print('定岗过程映射表中有，但是新增小而美三级岗位数据集没有的岗位(业务人员)：\n',error_3_2)
    
    
    # 2、 校验过程映射表 一二级是否满足现行版本
    
    # 获取本地小而美 201912~202012 月份版本的数表格比对
    xem_detail_Xem1 = set(xem_detail['xem_1'].tolist())
    xem_detail_Xem2 = set(xem_detail['xem_2'].tolist())
    
    # 定岗过程表，FS_xem_Level1 在小而美数仓一级中找不到，错误岗位
    error_3_3 = remark_3_df['job_name'][\
               (remark_3_df['FS_xem_Level1'].apply(lambda x: True if x not in xem_detail_Xem1 else False )).values].tolist()
    
    error_3_4 = remark_3_df['job_name'][\
               (remark_3_df['FS_xem_Level2'].apply(lambda x: True if x not in xem_detail_Xem2 else False )).values].tolist()
    print(f'小而美一级在现行版本一级表并没有找到，错误岗位：\n{error_3_3}' if len(error_3_3) != 0 else '一级正确')
    print(f'小而美二级在现行版本二级表并没有找到，错误岗位：\n{error_3_4}' if len(error_3_3) != 0 else '二级正确')






 



            
            
     



    
     
        

 









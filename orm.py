import asyncio,logging,aiomysql

@asyncio.coroutine
#sql语句反馈
def log(sql,args=()):
    logging.info('sql:%s'%sql)

#创建一个连接池
@asyncio.coroutine
def create_pool(loop, **kw):
    logging.info('create database connection pool。。。')
    global __pool   #将__pool变为全局变量
    __pool=yield from aiomysql.create_pool(
        host=kw.get('host','localhost'),
        port=kw.get('port',3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset','utf8'),
        autocommit=kw.get('autocommit',True),
        maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),
        loop=loop

    )

#单独封装selsct
@asyncio.coroutine
def select(sql,args,size=None):
    log(sql,args)
    global __pool
    with (yield from __pool) as conn:
        cur=yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?','%s'),args or ())
        if size:
            rs=yield from cur.fetchmany(size)
        else :
            rs=yield  from  cur.fetchall()
        yield  from cur.close()
        logging.info('rows return : %s'%len(rs))
        return rs

#封装insert ，update，delete
#语句操作参数一样，所以定义一个通用执行函数，但是执行格式不一样
#返回操作影响的行号
@asyncio.coroutine
def execute(sql,args,autocommit=True):
    log(sql)
    with(yield from __pool) as conn:
        try:
            cur=yield from conn.cursor()
            yield from cur.execute(sql.replace('?','%s'),args)
            affected=cur.rowcount
            yield from cur.close()
        except BaseException as e:
            raise
        return affected
#创建占位符
def create_string(num):
    l=[]
    for i in range(num):
        l.append('?')
    return  ','.join(l)

#定义field类，负责保存数据库的字段名和字段类型
class Field(object):
    #表的字段包含名字，类型，是否为表的主键和默认值
    def __init__(self,name,column_type,primary_key,default):
        self.name=name
        self.column_type=column_type
        self.primary_key=primary_key
        self.default=default
    #返回表名，字段名，字段类型
    def __str__(self):
        return "<%s ,%s ,%s>" %(self.__class__.__name__,self.name,self.column_type)

#定义数据库中的五个存储类型
#字符型
class StringField(Field):
    def __init__(self,name=None,primary_key=False,default=None,ddl='varchar(100)'):
        super().__init__(name,primary_key,default,ddl)

#布尔型
class BooleanFiled(Field):
    def __init__(self,name=None,default=False):
        super().__init__(name,default)

#整型
class IntegerFiled(Field):
    def __init__(self,name=None,primary_key=False,default=0):
        super ().__init__(name,'int',primary_key,default)

#浮点型
class FloatFiled(Field):
    def __init__(self,name=None,primary_key=False,default=0.0):
        super().__init__(name,'float',primary_key,default)

#文本型
class TextFiled(Field):
    def __init__(self,name=None,default=None):
        super().__init__(name,'text',False,default)


#ModelMetaclass 类定义了所有 Model基类的子类实现的操作
#将数据库表名保存到__table__中
#mateclass是类的模板，所以用type类型

class ModelMetaclass(type):
#__new__控制__init__的执行
    def __new__(cls,name,bases,attrs):#bases代表继承父类的集合，attrs代表类的方法集合
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)
        table_name=attrs('__table__',None) or name
        logging.info('found model:,%s (table: %s)' %(name,table_name))
        mappings=dict()
        fields=[]
        primarykey=None
        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info(' found mappinigs: %s ==> %s'%(k,v))
                mappings[k]=v
                if v.primary_key:
                    logging.info('found primary key %s'%k)
                    if primarykey:
                        raise RuntimeError('key for filed')
                    primarykey=k
                else:
                    fields.append(k)
        if not primarykey:
            raise RuntimeError('Primary key not found!')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields=list(map(lambda f:'`%s`' %f,fields))
        attrs['__mappings__']=mappings
        attrs['__table__']=table_name
        #保存主键名称
        attrs['__primary_key__']=primarykey
        #保存主键外的属性名
        attrs['__fields__']=fields
        #构建默认的增删改查语句
        attrs['__select__']='select `%s` , %s from `%s` '%(primarykey,','.join(escaped_fields),table_name)
        attrs['__insert__'] = 'insert into  `%s` (%s, `%s`) values (%s) '\
        % (table_name, ', '.join(escaped_fields), primarykey, create_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s` = ?' % (
        table_name, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primarykey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (table_name, primarykey)

#model从dict继承，具有字典的所以功能，同时实现特殊方法__getattr__和__setattr__,能实现属性操作
#实现数据库的所以方法，定义为class方法
class Model(dict,metaclass=ModelMetaclass):
    def __init__(self,**kw):
        super(Model,self).__init__(**kw)
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'"%key)

    def __setattr__(self, key, value):
        self[key]=value
    def getValue(self,key):
        return getattr(self,key,None)
    def getValueOrDefault(self,key):
        value=getattr(self,key,None)
        if value is None:
            field=self.__mappings__[key]
            if field.default is not None:
                value=field.default() if callable(field.default) else field.default
                logging.info('useing default value for %s :%s'%(key,str(value)))
                setattr(self,key,value)
        return value
    @classmethod
    def findAll(cls,where=None,args=None, **kw):
        sql=[cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args=[]
            orderBy=kw.get('orderBy',None)
            if orderBy:
                sql.append('order by')
                sql.append(orderBy)
            limit=kw.get('limit',None)
            if limit is not None:
                sql .append('limit')
                if isinstance(limit ,int):
                    sql.append('?')
                    args.append(limit)
                elif isinstance(limit ,tuple) and len(limit)==2:
                    sql.append('?,?')
                    args.extend(limit)
                else:
                    raise ValueError('invalid limit value: %s' %str(limit))
            rs=yield  from select(' '.join(sql),args)
            return [cls(**r) for r in rs]
    @classmethod
    def findnumber(cls,selectFiled,where=None,args=None):
            sql=['select %s _num_ from `%s`' %(selectFiled,cls.__table__)]
            if where :
                sql.append('where')
                sql.append(where)
            rs=yield from select(' '.join(sql),args,1)
            if len(rs)==0:
                return None
            return rs[0]['_num_']
    @classmethod
    def find(cls,pk):
        rs=yield  from select('%s where `%s`=?'%(cls.__select__, cls.__primary_key__),[pk],1)
        if len(rs)==0:
            return None
        return cls(**rs[0])
    def save(self):
        args=list(map(self.getValueOrDefault,self.__fileds__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows=yield from execute(self.__insert__,args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' %rows)
    def update(self):
        args=list(map(self.getValue,self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = yield from execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = yield from execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)


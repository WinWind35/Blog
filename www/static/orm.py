#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio,logging

import aiomysql

@asyncio.coroutine
async def create_pool(loop,**kw):   #创建连接池
    logging.info('create database connection pool...')
    global __pool   #全局变量
    __pool = await aiomysql.create_pool(
        host = kw.get('host','localhost'),
        port = kw.get('port',3306),
        user = kw['root'],
        password = kw['password'],
        db = kw['db'],
        charset = kw.get('charset','utf8'),
        autocommit = kw.get('autocommit',True),
        maxsize=kw.get('maxsize',10),
        minsize=kw.get('minsize',1),
        loop=loop
    )
    
@asyncio.coroutine
async def select(sql,args,size=None):
    logging.log(sql,args)
    global __pool
    with ( await __pool) as  conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?','%s'),args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs
@asyncio.coroutine
async def excute(sql,args):
    logging.log(sql, args)
    with(await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?','%s'),args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected

# 根据输入的参数生成占位符列表
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    # 以','为分隔符，将列表合成字符串
    return (','.join(L))

class Field(object):

    def __init__(self,name,column_type,primary_key,default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s,%s:%s>' % (self.__class__.__name__,self.column_type,self.default)


# -*- 定义不同类型的衍生Field -*-
# -*- 表的不同列的字段的类型不一样

class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, column_type='varchar(100)'):
        super().__init__(name, column_type, primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'boolean', False, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)


class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'Text', False, default)


# -*-定义Model的元类

# 所有的元类都继承自type
# ModelMetaclass元类定义了所有Model基类(继承ModelMetaclass)的子类实现的操作

# -*-ModelMetaclass的工作主要是为一个数据库表映射成一个封装的类做准备：
# ***读取具体子类(user)的映射信息
# 创造类的时候，排除对Model类的修改
# 在当前类中查找所有的类属性(attrs)，如果找到Field属性，就将其保存到__mappings__的dict中，同时从类属性中删除Field(防止实例属性遮住类的同名属性)
# 将数据库表名保存到__table__中

# 完成这些工作就可以在Model中定义各种数据库的操作方法
class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        # 排除Model类本身：
        if name == 'Model':
            return type.__new__(cls,name,bases,attrs)
        # 获取table名称：
        tableName = attrs.get('__table__',None) or name
        logging.info('found model: %s (table: %s)' % (name,tableName))
        # 获取所有的Field和主键名：
        mappings = dict()
        fields = []
        primaryKey = None
        for k,v in attrs.items():
            if isinstance(v, Field):
                # 此处打印的k是类的一个属性，v是这个属性在数据库中对应的Field列表属性
                logging.info('  found mapping: %s ==> %s' % (k, v))
                mappings[k] = v
                # 找到了主键
                if v.primary_key:
                    # 如果此时类实例已经存在主键，说明主键重复了
                    if primaryKey:
                        raise RuntimeError('Duplicate primary key for field: %s' % k)
                    # 否则将此列设为列表的主键
                    primaryKey = k
                else:
                    fields.append(k)
        # end for

        if not primaryKey:
            raise RuntimeError('Primary key not found.')

        # 从类属性中删除Field属性
        for k in mappings.keys():
            attrs.pop(k)
        # 保持除主键外的属性名为``(运算出字符串)列表形式
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保持属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的SELECT，INSERT，UPDATE和DELETE语句：
        # ``反引号功能同repr()`i`
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey,', '.join(escaped_fields),tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields),primaryKey,create_args_string(len(escaped_fields) +1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName,primaryKey)
        return type.__new__(cls, name, bases, attrs)


# 定义ORM所有映射的基类：Model
# Model类的任意子类可以映射一个数据库表
# Model类可以看作是对所有数据库表操作的基本定义的映射


# 基于字典查询形式
# Model从dict继承，拥有字典的所有功能，同时实现特殊方法__getattr__和__setattr__，能够实现属性操作
# 实现数据库操作的所有方法，定义为class方法，所有继承自Model都具有数据库操作方法
class Model(dict,metaclass=ModelMetaclass):

    def __init__(self,**kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return  self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self,key):
        return getattr(self,key,None)

    def getValueOrDefault(self,key):
        value = getattr(self,key,None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s:%s' % (key,str(value)))
                setattr(self,key,value)
        return value


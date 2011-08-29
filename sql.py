import sqlite3

class SQLDB:
    def __init__(self, path):
        self.map_type = dict(str='text', int='integer', float='real', bool='integer')
        self.conn = sqlite3.connect(path)
        self.conn.text_factory = str
        self.c = self.conn.cursor()

    def __create_table(self, table):
        sql = '''        
            CREATE TABLE %s (
              id INTEGER PRIMARY KEY DEFAULT NULL
            );
            ''' % (table, )
        self.c.execute(sql)
        self.conn.commit()
     
       
    def __create_table_if_not_exists(self, table):
        try:
            self.__get_table_schema(table)
        except Exception:
            self.__create_table(table)
           
    def __check_if_exists(self, table, field, value):
        sql = 'select * from %s where %s = ?' % (table, field)
        self.c.execute(sql, (value, ))
        result = self.c.fetchone()
        if not result: return False
        return result[0]
    
    def __get_table_schema(self, table):
        sql = 'select sql from sqlite_master where type = \'table\' and name = ?'
        self.c.execute(sql, (table, ))
        result = self.c.fetchone()
        if not result: raise Exception('Table doesn\'t exists')
        return result[0]
    
    def __add_column(self, table, field, field_type):
        sql = 'alter table %s add %s %s;' % (table, field, field_type)
        self.c.execute(sql)
        self.conn.commit()
    
    def __reorder_values(self, table, data):
        keys = []
        values = []
        schema = self.__get_table_schema(table)
        tmp = schema.split('(')[1].split(')')[0].split(',')
        for t in tmp:
            r_key = t.strip().split(' ')[0]
            keys.append(r_key)
            if r_key in data.keys():
                value = data[r_key]
                if type(value) == type(True):
                    value = value == True and 1 or 0
                values.append(value)
            else:
                values.append(None)
        return keys, values
    
    def __alter_table(self, table, keys, data):
        new_keys = [k for k in data.keys() if k not in keys]
        for field in new_keys:
            field_type = self.map_type[type(field).__name__] + ' default null'
            try:self.__add_column(table, field, field_type)
            except:pass
    
    def drop_tables(self):
        sql = 'select name from sqlite_master where type = \'table\''
        self.c.execute(sql)
        tables = self.c.fetchall()
        for table in tables:
            self.drop_table(table[0])
    
    def drop_table(self, table):
        sql = 'drop table if exists ' + table
        self.c.execute(sql)
        self.conn.commit()
        
    def save(self, table, data, check_field=None, table_should_exists=False):
        if not table_should_exists: self.__create_table_if_not_exists(table)
        keys, values = self.__reorder_values(table, data)
        self.__alter_table(table, keys, data)
        keys, values = self.__reorder_values(table, data)
        tmp = ('?, ' * len(keys))[:-2]
        
        if check_field: field_id = self.__check_if_exists(table, check_field, data[check_field])
        if check_field and field_id:
            values = values[1:]
            keys.remove('id')
            fields = ', '.join([k+' = ?' for k in keys])
            sql = 'update %s set %s where %s = ?' % (table, fields, check_field)
            values.append(data[check_field])
            self.c.execute(sql, values)
            self.conn.commit()
        else:    
            sql = 'insert into %s values (%s)' % (table, tmp)            
            self.c.execute(sql, values)
            self.conn.commit()
            sql = 'select max(id) from %s' % (table,)
            self.c.execute(sql)
            field_id = self.c.fetchone()[0]
        return field_id

    def close(self):
        self.conn.close()

if __name__ == '__main__':
    db = SQLDB('test.sqlite')
    db.save('LOW', dict(nome='Luca', isgay=False), 'nome')  
    db.close() 

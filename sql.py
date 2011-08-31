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
        fields = []
        values = []
        schema = self.__get_table_schema(table)
        tmp = schema.split('(')[1].split(')')[0].split(',')
        for t in tmp:
            r_key = t.strip().split(' ')[0]
            fields.append(r_key)
            if r_key in data.keys():
                value = data[r_key]
                if type(value) == type(True):
                    value = value == True and 1 or 0
                if type(value) == type(dict()):
                    value = self.__get_id(value)
                values.append(value)
            else:
                values.append(None)
        return fields, values
    
    def __get_id(self, data):
        sql = 'select id from %s where %s = ?' % (data['table'], data['match_field'])
        self.c.execute(sql, (data['match_value'],))
        result = self.c.fetchone()
        if not result: raise Exception('Table doesn\'t exists')
        return result[0]
    
    def __alter_table(self, table, fields, data):
        fields_to_add = [k for k in data.keys() if k not in fields]
        for field in fields_to_add:
            field_type = self.map_type[type(field).__name__] + ' default null'
            self.__add_column(table, field, field_type)
        return fields_to_add
    
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
        
    def save(self, table, data, match_field=None, table_should_exists=False):
        if not table_should_exists: self.__create_table_if_not_exists(table)
        fields, values = self.__reorder_values(table, data)
        fields_to_add = self.__alter_table(table, fields, data)
        fields.extend(fields_to_add)
        values.extend([data[f] for f in fields_to_add])
        tmp = ('?, ' * len(fields))[:-2]
        
        if match_field: field_id = self.__check_if_exists(table, match_field, data[match_field])
        if match_field and field_id:
            values = values[1:]
            fields.remove('id')
            tmp = ', '.join([k+' = ?' for k in fields])
            sql = 'update %s set %s where %s = ?' % (table, tmp, match_field)
            values.append(data[match_field])
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
    db.save('Condominio', dict(via='valgardena'))
    db.save('LOW', dict(nome='Denni', condominio_id=dict(table='Condominio', match_field='via', match_value='valgardena')), 'nome')  
    db.close() 

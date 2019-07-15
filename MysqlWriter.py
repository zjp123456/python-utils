class MysqlWriter(object):
  def save_data(self,table_name, data_dict):
      conn = MySQLdb.connect(host="localhost", user="root", passwd="root", db="test", charset="utf8")
      try:
          data_values = "(" + "%s," * (len(data_dict)) + ")"
          data_values = data_values.replace(',)', ')')

          dbField = data_dict.keys()
          dataTuple = tuple(data_dict.values())
          dbField = str(tuple(dbField)).replace("'", '')

          cursor = conn.cursor()
          sql = """ insert into %s %s values %s """ % (table_name, dbField, data_values)
          params = dataTuple
          cursor.execute(sql, params)
          conn.commit()
          
          return True

      except Exception as e:
          print e
          return False
      finally:
          cursor.close()
          conn.close()

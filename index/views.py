from django.shortcuts import render
import db
import sql 

# Create your views here.
def index(request):
    return render(request,'index/index.html')

def sql_view(request):
    sql_str = "Input SQL will be shown here"

    if request.method == 'GET':
        data = {'sql':sql_str
            }
        return render(request,'index/sql.html', data)
    elif request.method == 'POST':
        if request.POST.get('sql'):
            sql_str = request.POST['sql']
        database = None
        success, err_msg = sql.exec_sql(sql_str, database)
        panel_msg = ""
        if success:
            panel_msg += "success"
        else:
            panel_msg += "error"

        data = {'sql':sql_str,
                'success':success,
                'panel_msg':panel_msg,
                'err_msg':err_msg
                }

        return render(request,'index/sql.html', data)

def table_view(request,table_name=None):
    table_names = db.get_all_table_names()
    if table_name == None:
        try:
            table_name = table_names[0]
        except:
            table_name = "Default table name"
    
    title, content = db.get_table(table_name)
    data = {'table_names':table_names,
            'table_name':table_name,
            'title':title,
            'content':content}
    return render(request,'index/table.html', data)
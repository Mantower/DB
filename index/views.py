from django.shortcuts import render
import db
import sql 

# Create your views here.
def index(request):
    return render(request,'index/index.html')

def sql_view(request):
    # to store input sql, make sure we receive the right input
    sql_str = "Input SQL will be shown here"

    if request.method == 'GET':
        data = {'sql':sql_str}
        return render(request,'index/sql.html', data)
    elif request.method == 'POST':
        if request.POST.get('sql'):
            sql_str = request.POST['sql']
        # the db we want to do action on
        database = None
        # apply sql to the database
        # (Bool,String) to indicate status of execution and error message
        success, err_msg = sql.exec_sql(sql_str, database)

        # additional message to indicate the execution is successful or not
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
    # the db we want to view
    database = None
    # retreive all table names
    table_names = db.get_all_table_names(database)

    # if user doesn't specify which table to view, choose the first one as default
    if table_name == None:
        try:
            table_name = table_names[0]
        except:
            table_name = "Default table name"
    
    title, content = db.get_table(table_name, database)
    data = {'table_names':table_names,
            'table_name':table_name,
            'title':title,
            'content':content}
    return render(request,'index/table.html', data)
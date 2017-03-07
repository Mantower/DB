from django.shortcuts import render
import db

# Create your views here.
def index(request):
    return render(request,'index/index.html')

def sql(request):
    return render(request,'index/sql.html')

def table(request,table_name=None):
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
from django.shortcuts import render

# Create your views here.
def index(request):
    return render(request,'index/index.html')

def sql(request):
    return render(request,'index/sql.html')

def table(request):
    return render(request,'index/table.html')
from django.contrib import admin
from report import models

# Register your models here.


class ReportAdmin(admin.ModelAdmin):
    list_display = ['name', 'dataset', 'prefix']


class ReportDimensionAdmin(admin.ModelAdmin):

    list_display = ['name', 'report', 'key', 'query']
    list_filter = ['report']

    def __init__(self, *args, **kwargs):
        super(ReportDimensionAdmin, self).__init__(*args, **kwargs)


class ReportMetericAdmin(admin.ModelAdmin):

    list_display = ['name', 'report', 'key', 'query']
    list_filter = ['report']

    def __init__(self, *args, **kwargs):

        super(ReportMetericAdmin, self).__init__(*args, **kwargs)

class ReportColAdmin(admin.ModelAdmin):
    list_display = ['name', 'report', 'key', 'query']
    list_filter = ['report']

class TableAdmin(admin.ModelAdmin):
    list_display = [ 'id', 'report', 'key']

#admin.site.register(models.ReportTag)
admin.site.register(models.Report, ReportAdmin)
admin.site.register(models.Table, TableAdmin)
#admin.site.register(models.ReportCol, ReportColAdmin)
admin.site.register(models.ReportDimension, ReportDimensionAdmin)
admin.site.register(models.ReportMeteric, ReportMetericAdmin)

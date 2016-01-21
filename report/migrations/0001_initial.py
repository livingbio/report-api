# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations, models
import django.core.validators


class Migration(migrations.Migration):

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Filter',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=1024)),
                ('key', models.CharField(max_length=1024)),
                ('query_template', models.TextField()),
                ('description', models.TextField()),
                ('example', models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name='Report',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=1024)),
                ('prefix', models.CharField(unique=True, max_length=255, validators=[django.core.validators.RegexValidator(regex=b'.*___.*', inverse_match=True), django.core.validators.RegexValidator(regex=b'[a-zA-Z-0-9]+')])),
                ('live', models.BooleanField(default=True)),
                ('description', models.TextField(null=True, blank=True)),
            ],
        ),
        migrations.CreateModel(
            name='ReportApi',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=1024)),
                ('mode', models.CharField(default=b'ExportReportApi', max_length=1024, choices=[(b'TimeReportApi', b'TimeReportApi'), (b'ExportReportApi', b'ExportReportApi')])),
                ('live', models.BooleanField(default=True)),
                ('description', models.TextField()),
                ('query_template', models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name='ReportCol',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=1024)),
                ('key', models.CharField(max_length=1024)),
                ('type', models.CharField(max_length=100, choices=[(b'meteric', b'meteric'), (b'dimension', b'dimension')])),
                ('query', models.CharField(max_length=1024)),
                ('report', models.ForeignKey(related_name='cols', blank=True, to='report.Report', null=True)),
            ],
        ),
        migrations.CreateModel(
            name='ReportGroup',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=1024)),
                ('description', models.TextField(max_length=1024)),
                ('live', models.BooleanField(default=True)),
                ('key', models.CharField(max_length=1024)),
            ],
        ),
        migrations.CreateModel(
            name='ReportTag',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('name', models.CharField(max_length=1024)),
                ('description', models.TextField(max_length=1024)),
                ('apis', models.ManyToManyField(to='report.Report')),
            ],
        ),
        migrations.CreateModel(
            name='Table',
            fields=[
                ('id', models.AutoField(verbose_name='ID', serialize=False, auto_created=True, primary_key=True)),
                ('key', models.CharField(max_length=1024, validators=[django.core.validators.RegexValidator(b'[a-zA-Z-0-9]*')])),
                ('report', models.ForeignKey(related_name='tables', to='report.Report')),
            ],
        ),
        migrations.AddField(
            model_name='reportapi',
            name='cols',
            field=models.ManyToManyField(to='report.ReportCol'),
        ),
        migrations.AddField(
            model_name='reportapi',
            name='custom_filters',
            field=models.ManyToManyField(to='report.Filter', null=True, blank=True),
        ),
        migrations.AddField(
            model_name='report',
            name='apis',
            field=models.ManyToManyField(to='report.ReportApi', null=True, blank=True),
        ),
        migrations.AddField(
            model_name='report',
            name='group',
            field=models.ForeignKey(blank=True, to='report.ReportGroup', null=True),
        ),
        migrations.AddField(
            model_name='filter',
            name='col',
            field=models.ForeignKey(blank=True, to='report.ReportCol', null=True),
        ),
        migrations.CreateModel(
            name='ReportDimension',
            fields=[
            ],
            options={
                'verbose_name': 'dimension',
                'proxy': True,
            },
            bases=('report.reportcol',),
        ),
        migrations.CreateModel(
            name='ReportMeteric',
            fields=[
            ],
            options={
                'verbose_name': 'meteric',
                'proxy': True,
            },
            bases=('report.reportcol',),
        ),
        migrations.AlterUniqueTogether(
            name='table',
            unique_together=set([('report', 'key')]),
        ),
        migrations.AlterUniqueTogether(
            name='reportcol',
            unique_together=set([('report', 'key', 'type')]),
        ),
    ]

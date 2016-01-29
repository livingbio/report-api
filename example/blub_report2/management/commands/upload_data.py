from django.core.management.base import BaseCommand
from report.utils import upload
from datetime import datetime

date_type = lambda s: datetime.strptime(s, '%Y-%m-%d').date()


class Command(BaseCommand):
    help = 'backupd db'

    def add_arguments(self, parser):
        parser.add_argument('filename', nargs=1, type=str)
        parser.add_argument('name', nargs=1, type=str)
        parser.add_argument('date', nargs=1, type=date_type)

    def handle(self, *args, **options):
        upload(options['filename'][0], options['name'][0], options['date'][0])
